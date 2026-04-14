import { randomUUID } from 'crypto';
import { EntityManager, LockMode } from '@mikro-orm/core';
import { Injectable } from '@nestjs/common';
import { InboxMessage } from '../../domain/messaging/inbox-message';
import { OutboxMessage } from '../../domain/messaging/outbox-message';
import { computeWagerPayloadHash } from '../../domain/shared/canonical-json';
import {
  DomainError,
  EntityNotFoundError,
  FailureCode,
  IdempotencyConflictError,
  InsufficientFundsError,
  InvalidTransactionPropsError,
} from '../../domain/shared/domain-errors';
import { Money, MoneyProps } from '../../domain/shared/money';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../domain/wagering/wager-transaction';
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from '../../domain/wagering/wagering-events';
import { Wallet, WalletStatus } from '../../domain/wallet/wallet';
import {
  LedgerDirection,
  WalletLedgerEntry,
} from '../../domain/wallet/wallet-ledger-entry';
import { MetricsService } from '../../infrastructure/observability/metrics.service';
import { StructuredLogger } from '../../infrastructure/observability/structured-logger';
import {
  InboxMessageOrmEntity,
  WagerTransactionOrmEntity,
  WalletOrmEntity,
} from '../../infrastructure/persistence/entities/orm-entities';
import { DomainPersistenceMapper } from '../../infrastructure/persistence/mappers/domain-persistence.mapper';

export interface SubmitWagerTransactionInput {
  idempotencyKey: string;
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
  correlationId?: string;
  inboxContext?: {
    consumerName: string;
    messageId: string;
    payloadHash: string;
  };
}

export interface WagerTransactionResultDto {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  status: WagerTransactionStatus;
  money: MoneyProps;
  balance: MoneyProps;
  failureCode?: FailureCode;
  referenceExternalTransactionId?: string;
  referenceTransactionId?: string;
  idempotentReplay: boolean;
  createdAt: string;
  processedAt?: string;
}

@Injectable()
export class ProcessWagerTransactionUseCase {
  constructor(
    private readonly em: EntityManager,
    private readonly logger: StructuredLogger,
    private readonly metrics: MetricsService,
  ) {}

  async execute(input: SubmitWagerTransactionInput): Promise<WagerTransactionResultDto> {
    const startTime = performance.now();
    const correlationId = input.correlationId ?? randomUUID();

    if (!input.idempotencyKey || input.idempotencyKey.trim().length === 0) {
      throw new InvalidTransactionPropsError(
        'MISSING_IDEMPOTENCY_KEY',
        'Idempotency-Key is required',
      );
    }

    const idempotencyKey = input.idempotencyKey.trim();
    const money = Money.from(input.money);
    const payloadHash = computeWagerPayloadHash({
      providerId: input.providerId,
      externalTransactionId: input.externalTransactionId,
      playerId: input.playerId,
      walletId: input.walletId,
      roundId: input.roundId,
      gameId: input.gameId,
      kind: input.kind,
      money: money.toJSON(),
      referenceExternalTransactionId: input.referenceExternalTransactionId,
    });

    // Validate domain construction upfront (rejects OPENING, missing reference on REFUND/ROLLBACK, etc.)
    const candidateTx = WagerTransaction.create({
      providerId: input.providerId,
      externalTransactionId: input.externalTransactionId,
      idempotencyKey,
      payloadHash,
      walletId: input.walletId,
      playerId: input.playerId,
      roundId: input.roundId,
      gameId: input.gameId,
      kind: input.kind,
      money,
      referenceExternalTransactionId: input.referenceExternalTransactionId,
    });

    const maxRetries = 4;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const result = await this.executeInTransaction(
          candidateTx,
          input,
          payloadHash,
          correlationId,
        );
        const durationMs = performance.now() - startTime;
        this.metrics.recordProcessingLatency(durationMs);
        return result;
      } catch (err: unknown) {
        if (this.isUniqueOrSerializationError(err)) {
          this.metrics.incrementLockConflict();
          // Check if it was an idempotent concurrent insert that already succeeded
          const replayResult = await this.tryResolveConcurrentIdempotency(
            idempotencyKey,
            input.providerId,
            input.externalTransactionId,
            payloadHash,
            Boolean(input.inboxContext),
          );
          if (replayResult) {
            const durationMs = performance.now() - startTime;
            this.metrics.recordProcessingLatency(durationMs);
            return replayResult;
          }

          if (attempt < maxRetries) {
            this.metrics.incrementRetry('db_serialization');
            await this.sleep(15 * attempt);
            continue;
          }
        }
        throw err;
      }
    }

    throw new DomainError('CONCURRENCY_RETRY_EXHAUSTED', 'Failed after concurrency retries');
  }

  /**
   * Reprocesses a single PENDING_REFERENCE transaction inside a locked wallet transaction.
   * Used by the scheduled PendingReferenceWorker (Section 7.1).
   */
  async reprocessPendingReference(
    transactionId: string,
    maxAttempts: number,
    baseDelayMs: number,
    correlationId = randomUUID(),
  ): Promise<WagerTransactionResultDto | null> {
    return this.em.transactional(async (txEm) => {
      const txOrm = await txEm.findOne(
        WagerTransactionOrmEntity,
        { id: transactionId },
        { lockMode: LockMode.PESSIMISTIC_WRITE },
      );

      if (!txOrm) {
        return null;
      }

      const tx = DomainPersistenceMapper.toDomainWagerTransaction(txOrm);
      if (tx.status !== WagerTransactionStatus.PendingReference) {
        return null;
      }

      const walletOrm = await txEm.findOne(
        WalletOrmEntity,
        { id: tx.walletId },
        { lockMode: LockMode.PESSIMISTIC_WRITE },
      );

      if (!walletOrm) {
        return null;
      }

      const wallet = DomainPersistenceMapper.toDomainWallet(walletOrm);
      const now = new Date();

      const refOrm = tx.referenceExternalTransactionId
        ? await txEm.findOne(WagerTransactionOrmEntity, {
            providerId: tx.providerId,
            externalTransactionId: tx.referenceExternalTransactionId,
          })
        : null;

      if (!refOrm) {
        if (tx.retryCount + 1 >= maxAttempts) {
          tx.reject(FailureCode.ReferenceNotFound, now, wallet.balance);
          DomainPersistenceMapper.toOrmWagerTransaction(tx, txOrm);

          const rejectedEvent = WagerTransactionRejected.from(
            tx,
            wallet.balance.toJSON(),
            { correlationId, occurredAt: now },
          );
          txEm.persist(
            DomainPersistenceMapper.toOrmOutboxMessage(
              OutboxMessage.enqueue(rejectedEvent),
            ),
          );

          await txEm.flush();
          this.metrics.incrementTransactionStatus(tx.status, tx.kind);
          this.logger.warn('wager.pending_reference.expired_rejected', {
            correlationId,
            transactionId: tx.id,
            walletId: tx.walletId,
            providerId: tx.providerId,
            failureCode: tx.failureCode,
          });
          return this.toResultDto(tx, wallet.balance, false);
        }

        tx.scheduleReferenceRetry(now, baseDelayMs);
        DomainPersistenceMapper.toOrmWagerTransaction(tx, txOrm);
        await txEm.flush();
        this.metrics.incrementRetry('pending_reference');
        return this.toResultDto(tx, wallet.balance, false);
      }

      const referenceTx = DomainPersistenceMapper.toDomainWagerTransaction(refOrm);
      if (
        referenceTx.status === WagerTransactionStatus.Pending ||
        referenceTx.status === WagerTransactionStatus.PendingReference
      ) {
        if (tx.retryCount + 1 >= maxAttempts) {
          tx.reject(FailureCode.ReferenceNotFound, now, wallet.balance);
          DomainPersistenceMapper.toOrmWagerTransaction(tx, txOrm);
          const rejectedEvent = WagerTransactionRejected.from(
            tx,
            wallet.balance.toJSON(),
            { correlationId, occurredAt: now },
          );
          txEm.persist(
            DomainPersistenceMapper.toOrmOutboxMessage(
              OutboxMessage.enqueue(rejectedEvent),
            ),
          );
          await txEm.flush();
          this.metrics.incrementTransactionStatus(tx.status, tx.kind);
          return this.toResultDto(tx, wallet.balance, false);
        }

        tx.scheduleReferenceRetry(now, baseDelayMs);
        DomainPersistenceMapper.toOrmWagerTransaction(tx, txOrm);
        await txEm.flush();
        this.metrics.incrementRetry('pending_reference');
        return this.toResultDto(tx, wallet.balance, false);
      }

      // Reference exists and is in a terminal state: apply business rules & movement
      await this.applyTransactionWithResolvedReference(
        txEm,
        tx,
        txOrm,
        wallet,
        walletOrm,
        referenceTx,
        now,
        correlationId,
      );

      await txEm.flush();
      return this.toResultDto(tx, tx.observedBalance ?? wallet.balance, false);
    });
  }

  async getByTransactionId(transactionId: string): Promise<WagerTransactionResultDto> {
    const orm = await this.em.findOne(WagerTransactionOrmEntity, { id: transactionId });
    if (!orm) {
      throw new EntityNotFoundError('WagerTransaction', transactionId);
    }
    const tx = DomainPersistenceMapper.toDomainWagerTransaction(orm);
    const walletOrm = await this.em.findOne(WalletOrmEntity, { id: tx.walletId });
    const fallbackBalance = walletOrm
      ? DomainPersistenceMapper.toDomainWallet(walletOrm).balance
      : Money.zero(tx.money.currency);
    return this.toResultDto(tx, tx.observedBalance ?? fallbackBalance, false);
  }

  async getByProviderAndExternalId(
    providerId: string,
    externalTransactionId: string,
  ): Promise<WagerTransactionResultDto> {
    const orm = await this.em.findOne(WagerTransactionOrmEntity, {
      providerId,
      externalTransactionId,
    });
    if (!orm) {
      throw new EntityNotFoundError(
        'WagerTransaction',
        `${providerId}:${externalTransactionId}`,
      );
    }
    const tx = DomainPersistenceMapper.toDomainWagerTransaction(orm);
    const walletOrm = await this.em.findOne(WalletOrmEntity, { id: tx.walletId });
    const fallbackBalance = walletOrm
      ? DomainPersistenceMapper.toDomainWallet(walletOrm).balance
      : Money.zero(tx.money.currency);
    return this.toResultDto(tx, tx.observedBalance ?? fallbackBalance, false);
  }

  private async executeInTransaction(
    candidateTx: WagerTransaction,
    input: SubmitWagerTransactionInput,
    payloadHash: string,
    correlationId: string,
  ): Promise<WagerTransactionResultDto> {
    return this.em.transactional(async (txEm) => {
      const now = new Date();

      // 1. Check existing transaction by idempotencyKey or (providerId, externalTransactionId) first
      const preExistingOrm = await txEm.findOne(WagerTransactionOrmEntity, {
        $or: [
          { idempotencyKey: candidateTx.idempotencyKey },
          {
            providerId: candidateTx.providerId,
            externalTransactionId: candidateTx.externalTransactionId,
          },
        ],
      });

      if (preExistingOrm) {
        const existingTx = DomainPersistenceMapper.toDomainWagerTransaction(preExistingOrm);
        if (
          existingTx.idempotencyKey === candidateTx.idempotencyKey &&
          !existingTx.matchesPayload(payloadHash)
        ) {
          throw new IdempotencyConflictError(candidateTx.idempotencyKey);
        }
        if (
          existingTx.providerId === candidateTx.providerId &&
          existingTx.externalTransactionId === candidateTx.externalTransactionId &&
          !existingTx.matchesPayload(payloadHash)
        ) {
          throw new IdempotencyConflictError(candidateTx.idempotencyKey);
        }
      }

      // 2. If called from SQS consumer, check persistent Inbox
      let inboxOrm: InboxMessageOrmEntity | null = null;
      if (input.inboxContext) {
        inboxOrm = await txEm.findOne(InboxMessageOrmEntity, {
          consumerName: input.inboxContext.consumerName,
          messageId: input.inboxContext.messageId,
        });

        if (inboxOrm && inboxOrm.processedAt) {
          this.metrics.incrementDuplicateDetected('sqs');
          this.logger.info('sqs.inbox.duplicate_skipped', {
            correlationId,
            messageId: input.inboxContext.messageId,
            providerId: input.providerId,
            walletId: input.walletId,
          });
          if (preExistingOrm) {
            const existingTx = DomainPersistenceMapper.toDomainWagerTransaction(preExistingOrm);
            const walletOrm = await txEm.findOne(WalletOrmEntity, { id: existingTx.walletId });
            const fallbackBalance = walletOrm
              ? DomainPersistenceMapper.toDomainWallet(walletOrm).balance
              : Money.zero(existingTx.money.currency);
            return this.toResultDto(
              existingTx,
              existingTx.observedBalance ?? fallbackBalance,
              true,
            );
          }
        }

        if (!inboxOrm) {
          const inboxMsg = InboxMessage.receive({
            consumerName: input.inboxContext.consumerName,
            messageId: input.inboxContext.messageId,
            payloadHash: input.inboxContext.payloadHash,
            receivedAt: now,
          });
          inboxMsg.markProcessed(now);
          inboxOrm = DomainPersistenceMapper.toOrmInboxMessage(inboxMsg);
          txEm.persist(inboxOrm);
        } else {
          inboxOrm.processedAt = now;
        }
      }

      // 3. Acquire PESSIMISTIC_WRITE lock on the target Wallet (Unit of Concurrency = walletId)
      const walletOrm = await txEm.findOne(
        WalletOrmEntity,
        { id: candidateTx.walletId },
        { lockMode: LockMode.PESSIMISTIC_WRITE },
      );

      if (!walletOrm) {
        throw new EntityNotFoundError('Wallet', candidateTx.walletId);
      }

      const wallet = DomainPersistenceMapper.toDomainWallet(walletOrm);

      // 4. Re-check idempotency inside the wallet lock to guarantee zero race conditions
      const lockedExistingOrm = await txEm.findOne(WagerTransactionOrmEntity, {
        $or: [
          { idempotencyKey: candidateTx.idempotencyKey },
          {
            providerId: candidateTx.providerId,
            externalTransactionId: candidateTx.externalTransactionId,
          },
        ],
      });

      if (lockedExistingOrm) {
        const existingTx = DomainPersistenceMapper.toDomainWagerTransaction(lockedExistingOrm);
        if (!existingTx.matchesPayload(payloadHash)) {
          throw new IdempotencyConflictError(candidateTx.idempotencyKey);
        }

        if (!input.inboxContext) {
          this.metrics.incrementDuplicateDetected('http');
        }
        this.logger.info('wager.transaction.idempotent_replay', {
          correlationId,
          messageId: input.inboxContext?.messageId,
          transactionId: existingTx.id,
          walletId: existingTx.walletId,
          providerId: existingTx.providerId,
          status: existingTx.status,
        });

        return this.toResultDto(
          existingTx,
          existingTx.observedBalance ?? wallet.balance,
          true,
        );
      }

      const txOrm = DomainPersistenceMapper.toOrmWagerTransaction(candidateTx);
      txEm.persist(txOrm);

      // 5. Validate wallet context (player ownership, currency, active status)
      if (wallet.playerId !== candidateTx.playerId) {
        await this.rejectAndEnqueue(
          txEm,
          candidateTx,
          txOrm,
          wallet,
          FailureCode.ReferenceContextMismatch,
          now,
          correlationId,
        );
        await txEm.flush();
        return this.toResultDto(candidateTx, wallet.balance, false);
      }

      if (wallet.currency !== candidateTx.money.currency) {
        await this.rejectAndEnqueue(
          txEm,
          candidateTx,
          txOrm,
          wallet,
          FailureCode.CurrencyMismatch,
          now,
          correlationId,
        );
        await txEm.flush();
        return this.toResultDto(candidateTx, wallet.balance, false);
      }

      if (wallet.status !== WalletStatus.Active) {
        await this.rejectAndEnqueue(
          txEm,
          candidateTx,
          txOrm,
          wallet,
          FailureCode.WalletNotActive,
          now,
          correlationId,
        );
        await txEm.flush();
        return this.toResultDto(candidateTx, wallet.balance, false);
      }

      // 6. Resolve reference if provided or required
      let referenceTx: WagerTransaction | undefined;
      if (candidateTx.referenceExternalTransactionId) {
        const refOrm = await txEm.findOne(WagerTransactionOrmEntity, {
          providerId: candidateTx.providerId,
          externalTransactionId: candidateTx.referenceExternalTransactionId,
        });

        if (
          !refOrm ||
          refOrm.status === WagerTransactionStatus.Pending ||
          refOrm.status === WagerTransactionStatus.PendingReference
        ) {
          const baseDelayMs = Number(process.env.PENDING_REF_BASE_DELAY_MS ?? 150);
          candidateTx.markPendingReference(new Date(now.getTime() + baseDelayMs));
          candidateTx.recordObservedBalance(wallet.balance);
          DomainPersistenceMapper.toOrmWagerTransaction(candidateTx, txOrm);

          const pendingRefEvent = WagerTransactionPendingReference.from(candidateTx, {
            correlationId,
            causationId: input.inboxContext?.messageId,
            occurredAt: now,
          });
          txEm.persist(
            DomainPersistenceMapper.toOrmOutboxMessage(
              OutboxMessage.enqueue(pendingRefEvent),
            ),
          );

          await txEm.flush();
          this.metrics.incrementTransactionStatus(candidateTx.status, candidateTx.kind);
          this.logger.info('wager.transaction.pending_reference', {
            correlationId,
            messageId: input.inboxContext?.messageId,
            transactionId: candidateTx.id,
            walletId: candidateTx.walletId,
            providerId: candidateTx.providerId,
            kind: candidateTx.kind,
          });
          return this.toResultDto(candidateTx, wallet.balance, false);
        }

        referenceTx = DomainPersistenceMapper.toDomainWagerTransaction(refOrm);
      }

      // 7. Apply business rules and financial effect
      await this.applyTransactionWithResolvedReference(
        txEm,
        candidateTx,
        txOrm,
        wallet,
        walletOrm,
        referenceTx,
        now,
        correlationId,
        input.inboxContext?.messageId,
      );

      await txEm.flush();
      return this.toResultDto(
        candidateTx,
        candidateTx.observedBalance ?? wallet.balance,
        false,
      );
    });
  }

  private async applyTransactionWithResolvedReference(
    txEm: EntityManager,
    tx: WagerTransaction,
    txOrm: WagerTransactionOrmEntity,
    wallet: Wallet,
    walletOrm: WalletOrmEntity,
    referenceTx: WagerTransaction | undefined,
    now: Date,
    correlationId: string,
    messageId?: string,
  ): Promise<void> {
    if (referenceTx) {
      // Rule 7.2: Reference must belong to the same provider, player, wallet, currency and round
      if (
        referenceTx.providerId !== tx.providerId ||
        referenceTx.playerId !== tx.playerId ||
        referenceTx.walletId !== tx.walletId ||
        referenceTx.money.currency !== tx.money.currency ||
        referenceTx.roundId !== tx.roundId
      ) {
        await this.rejectAndEnqueue(
          txEm,
          tx,
          txOrm,
          wallet,
          FailureCode.ReferenceContextMismatch,
          now,
          correlationId,
          messageId,
        );
        return;
      }

      // Rule 7.3: Validate allowed reference kind
      if (tx.kind === WagerTransactionKind.Refund) {
        if (referenceTx.kind !== WagerTransactionKind.Bet) {
          await this.rejectAndEnqueue(
            txEm,
            tx,
            txOrm,
            wallet,
            FailureCode.ReferenceKindInvalid,
            now,
            correlationId,
            messageId,
          );
          return;
        }
      } else if (tx.kind === WagerTransactionKind.Rollback) {
        if (
          referenceTx.kind !== WagerTransactionKind.Bet &&
          referenceTx.kind !== WagerTransactionKind.Win &&
          referenceTx.kind !== WagerTransactionKind.Refund
        ) {
          await this.rejectAndEnqueue(
            txEm,
            tx,
            txOrm,
            wallet,
            FailureCode.ReferenceKindInvalid,
            now,
            correlationId,
            messageId,
          );
          return;
        }
      } else if (
        tx.kind === WagerTransactionKind.Win ||
        tx.kind === WagerTransactionKind.Loss
      ) {
        if (referenceTx.kind !== WagerTransactionKind.Bet) {
          await this.rejectAndEnqueue(
            txEm,
            tx,
            txOrm,
            wallet,
            FailureCode.ReferenceKindInvalid,
            now,
            correlationId,
            messageId,
          );
          return;
        }
      }

      // Reference must be PROCESSED
      if (referenceTx.status !== WagerTransactionStatus.Processed) {
        await this.rejectAndEnqueue(
          txEm,
          tx,
          txOrm,
          wallet,
          FailureCode.ReferenceNotProcessed,
          now,
          correlationId,
          messageId,
        );
        return;
      }

      // Rules 7.4 & 7.5 for REFUND and ROLLBACK
      if (
        tx.kind === WagerTransactionKind.Refund ||
        tx.kind === WagerTransactionKind.Rollback
      ) {
        if (!tx.money.equals(referenceTx.money)) {
          await this.rejectAndEnqueue(
            txEm,
            tx,
            txOrm,
            wallet,
            FailureCode.ReferenceAmountMismatch,
            now,
            correlationId,
            messageId,
          );
          return;
        }

        // Rule 7.4: Cannot reverse the same reference twice
        const existingReversal = await txEm.findOne(WagerTransactionOrmEntity, {
          referenceTransactionId: referenceTx.id,
          status: WagerTransactionStatus.Processed,
          kind: { $in: [WagerTransactionKind.Refund, WagerTransactionKind.Rollback] },
        });

        if (existingReversal) {
          await this.rejectAndEnqueue(
            txEm,
            tx,
            txOrm,
            wallet,
            FailureCode.ReferenceAlreadyReversed,
            now,
            correlationId,
            messageId,
          );
          return;
        }
      }
    }

    // Apply financial movement on Wallet
    let ledgerEntry: WalletLedgerEntry | undefined;

    try {
      if (tx.affectsBalance()) {
        const direction = tx.ledgerDirectionFor(referenceTx);
        const isReversal =
          tx.kind === WagerTransactionKind.Refund ||
          tx.kind === WagerTransactionKind.Rollback;

        if (direction === LedgerDirection.Debit) {
          ledgerEntry = wallet.debit({
            transactionId: tx.id,
            amount: tx.money,
            occurredAt: now,
            isReversal,
          });
        } else {
          ledgerEntry = wallet.credit({
            transactionId: tx.id,
            amount: tx.money,
            occurredAt: now,
            isReversal,
          });
        }
      }
    } catch (err: unknown) {
      if (err instanceof InsufficientFundsError) {
        await this.rejectAndEnqueue(
          txEm,
          tx,
          txOrm,
          wallet,
          err.failureCode,
          now,
          correlationId,
          messageId,
        );
        return;
      }
      throw err;
    }

    tx.markProcessed(referenceTx?.id, now, wallet.balance);
    DomainPersistenceMapper.toOrmWagerTransaction(tx, txOrm);

    if (ledgerEntry) {
      DomainPersistenceMapper.toOrmWallet(wallet, walletOrm);
    }

    // Flush wager_transactions and wallets first inside the SQL transaction so the
    // foreign key wallet_ledger_entries.transaction_id -> wager_transactions.id is present
    await txEm.flush();

    if (ledgerEntry) {
      txEm.persist(DomainPersistenceMapper.toOrmLedgerEntry(ledgerEntry));
    }

    // Enqueue Outbox integration events in the exact same SQL transaction
    const processedEvent = WagerTransactionProcessed.from(tx, wallet, {
      correlationId,
      causationId: messageId,
      occurredAt: now,
    });
    txEm.persist(
      DomainPersistenceMapper.toOrmOutboxMessage(
        OutboxMessage.enqueue(processedEvent),
      ),
    );

    if (ledgerEntry) {
      const balanceChangedEvent = WalletBalanceChanged.from(wallet, ledgerEntry, {
        correlationId,
        causationId: messageId,
        occurredAt: now,
      });
      txEm.persist(
        DomainPersistenceMapper.toOrmOutboxMessage(
          OutboxMessage.enqueue(balanceChangedEvent),
        ),
      );
    }

    this.metrics.incrementTransactionStatus(tx.status, tx.kind);
    this.logger.info('wager.transaction.processed', {
      correlationId,
      messageId,
      transactionId: tx.id,
      walletId: tx.walletId,
      providerId: tx.providerId,
      kind: tx.kind,
      status: tx.status,
    });
  }

  private async rejectAndEnqueue(
    txEm: EntityManager,
    tx: WagerTransaction,
    txOrm: WagerTransactionOrmEntity,
    wallet: Wallet,
    failureCode: FailureCode,
    now: Date,
    correlationId: string,
    messageId?: string,
  ): Promise<void> {
    tx.reject(failureCode, now, wallet.balance);
    DomainPersistenceMapper.toOrmWagerTransaction(tx, txOrm);

    const rejectedEvent = WagerTransactionRejected.from(
      tx,
      wallet.balance.toJSON(),
      {
        correlationId,
        causationId: messageId,
        occurredAt: now,
      },
    );
    txEm.persist(
      DomainPersistenceMapper.toOrmOutboxMessage(
        OutboxMessage.enqueue(rejectedEvent),
      ),
    );

    this.metrics.incrementTransactionStatus(tx.status, tx.kind);
    this.logger.warn('wager.transaction.rejected', {
      correlationId,
      messageId,
      transactionId: tx.id,
      walletId: tx.walletId,
      providerId: tx.providerId,
      kind: tx.kind,
      status: tx.status,
      failureCode,
    });
  }

  private async tryResolveConcurrentIdempotency(
    idempotencyKey: string,
    providerId: string,
    externalTransactionId: string,
    payloadHash: string,
    fromSqs: boolean,
  ): Promise<WagerTransactionResultDto | null> {
    const forkEm = this.em.fork();
    const existingOrm = await forkEm.findOne(WagerTransactionOrmEntity, {
      $or: [
        { idempotencyKey },
        { providerId, externalTransactionId },
      ],
    });

    if (!existingOrm) {
      return null;
    }

    const existingTx = DomainPersistenceMapper.toDomainWagerTransaction(existingOrm);
    if (!existingTx.matchesPayload(payloadHash)) {
      throw new IdempotencyConflictError(idempotencyKey);
    }

    const walletOrm = await forkEm.findOne(WalletOrmEntity, {
      id: existingTx.walletId,
    });
    const fallbackBalance = walletOrm
      ? DomainPersistenceMapper.toDomainWallet(walletOrm).balance
      : Money.zero(existingTx.money.currency);

    this.metrics.incrementDuplicateDetected(fromSqs ? 'sqs' : 'http');
    return this.toResultDto(
      existingTx,
      existingTx.observedBalance ?? fallbackBalance,
      true,
    );
  }

  private isUniqueOrSerializationError(err: unknown): boolean {
    if (!err || typeof err !== 'object') {
      return false;
    }
    const pgCode = (err as { code?: string }).code;
    const message = ((err as Error).message ?? '').toLowerCase();
    return (
      pgCode === '23505' ||
      pgCode === '40001' ||
      pgCode === '40p01' ||
      message.includes('unique constraint') ||
      message.includes('duplicate key') ||
      message.includes('could not serialize') ||
      message.includes('deadlock')
    );
  }

  private toResultDto(
    tx: WagerTransaction,
    balance: Money,
    idempotentReplay: boolean,
  ): WagerTransactionResultDto {
    const dto: WagerTransactionResultDto = {
      transactionId: tx.id,
      providerId: tx.providerId,
      externalTransactionId: tx.externalTransactionId,
      idempotencyKey: tx.idempotencyKey,
      walletId: tx.walletId,
      playerId: tx.playerId,
      roundId: tx.roundId,
      gameId: tx.gameId,
      kind: tx.kind,
      status: tx.status,
      money: tx.money.toJSON(),
      balance: balance.toJSON(),
      idempotentReplay,
      createdAt: tx.createdAt.toISOString(),
    };

    if (tx.failureCode) {
      dto.failureCode = tx.failureCode;
    }
    if (tx.referenceExternalTransactionId) {
      dto.referenceExternalTransactionId = tx.referenceExternalTransactionId;
    }
    if (tx.referenceTransactionId) {
      dto.referenceTransactionId = tx.referenceTransactionId;
    }
    if (tx.processedAt) {
      dto.processedAt = tx.processedAt.toISOString();
    }

    return dto;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
