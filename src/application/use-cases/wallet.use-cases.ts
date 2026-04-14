import { randomUUID } from 'crypto';
import { EntityManager, LockMode } from '@mikro-orm/core';
import { Inject, Injectable, Optional } from '@nestjs/common';
import { OutboxMessage } from '../../domain/messaging/outbox-message';
import { computeWagerPayloadHash } from '../../domain/shared/canonical-json';
import {
  DuplicateWalletError,
  EntityNotFoundError,
} from '../../domain/shared/domain-errors';
import { Money, MoneyProps } from '../../domain/shared/money';
import {
  WagerTransaction,
  WagerTransactionKind,
} from '../../domain/wagering/wager-transaction';
import {
  WagerTransactionProcessed,
  WalletBalanceChanged,
} from '../../domain/wagering/wagering-events';
import { Wallet, WalletStatus } from '../../domain/wallet/wallet';
import { LedgerDirection } from '../../domain/wallet/wallet-ledger-entry';
import { MetricsService } from '../../infrastructure/observability/metrics.service';
import { StructuredLogger } from '../../infrastructure/observability/structured-logger';
import {
  OutboxMessageOrmEntity,
  WagerTransactionOrmEntity,
  WalletLedgerEntryOrmEntity,
  WalletOrmEntity,
} from '../../infrastructure/persistence/entities/orm-entities';
import { DomainPersistenceMapper } from '../../infrastructure/persistence/mappers/domain-persistence.mapper';
import {
  REALTIME_NOTIFIER_PORT,
  RealtimeNotifierPort,
} from '../ports/application.ports';

export interface CreateWalletInput {
  id?: string;
  playerId: string;
  initialBalance: MoneyProps;
  metadata?: Record<string, unknown>;
  correlationId?: string;
}

export interface WalletResponseDto {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
  status: WalletStatus;
  metadata?: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface LedgerCursorPayload {
  createdAt: string;
  id: string;
}

export interface LedgerPageResponseDto {
  walletId: string;
  items: Array<{
    id: string;
    walletId: string;
    transactionId: string;
    direction: LedgerDirection;
    money: MoneyProps;
    balanceBefore: MoneyProps;
    balanceAfter: MoneyProps;
    createdAt: string;
    doubleEntry: {
      debitAccount: string;
      creditAccount: string;
      amount: string;
      currency: string;
    };
  }>;
  nextCursor: string | null;
  hasMore: boolean;
}

export interface ReconciliationResponseDto {
  walletId: string;
  storedBalance: MoneyProps;
  calculatedBalance: MoneyProps;
  difference: MoneyProps;
  consistent: boolean;
  checkedEntries: number;
}

function encodeLedgerCursor(payload: LedgerCursorPayload): string {
  return Buffer.from(JSON.stringify(payload), 'utf8').toString('base64url');
}

function decodeLedgerCursor(cursor: string): LedgerCursorPayload {
  try {
    const raw = Buffer.from(cursor, 'base64url').toString('utf8');
    const parsed = JSON.parse(raw) as LedgerCursorPayload;
    if (!parsed || typeof parsed.createdAt !== 'string' || typeof parsed.id !== 'string') {
      throw new Error('Invalid cursor structure');
    }
    return parsed;
  } catch {
    throw new Error('INVALID_LEDGER_CURSOR');
  }
}

@Injectable()
export class WalletUseCases {
  constructor(
    private readonly em: EntityManager,
    private readonly logger: StructuredLogger,
    private readonly metrics: MetricsService,
    @Optional()
    @Inject(REALTIME_NOTIFIER_PORT)
    private readonly realtimeNotifier?: RealtimeNotifierPort,
  ) {}

  async createWallet(input: CreateWalletInput): Promise<WalletResponseDto> {
    const initialMoney = Money.from(input.initialBalance);
    const walletId = input.id ?? randomUUID();
    const correlationId = input.correlationId ?? randomUUID();

    try {
      const result = await this.em.transactional(async (txEm) => {
        const existing = await txEm.findOne(WalletOrmEntity, {
          playerId: input.playerId,
          currency: initialMoney.currency,
        });

        if (existing) {
          throw new DuplicateWalletError(input.playerId, initialMoney.currency);
        }

        const now = new Date();
        const wallet = Wallet.open({
          id: walletId,
          playerId: input.playerId,
          initialBalance: initialMoney,
          metadata: input.metadata,
          createdAt: now,
        });

        const walletOrm = DomainPersistenceMapper.toOrmWallet(wallet);
        txEm.persist(walletOrm);
        await txEm.flush();

        // Section 9: O saldo inicial, quando maior que zero, gera uma transação interna OPENING
        // na mesma transação SQL, com lançamento CREDIT correspondente no ledger.
        if (initialMoney.isPositive()) {
          const openingTxId = randomUUID();
          const idempotencyKey = `internal:opening:${wallet.id}`;
          const payloadHash = computeWagerPayloadHash({
            providerId: 'internal-system',
            externalTransactionId: `opening-${wallet.id}`,
            playerId: wallet.playerId,
            walletId: wallet.id,
            roundId: `opening-round-${wallet.id}`,
            gameId: 'wallet-opening',
            kind: WagerTransactionKind.Opening,
            money: initialMoney.toJSON(),
          });

          const openingTx = WagerTransaction.create({
            id: openingTxId,
            providerId: 'internal-system',
            externalTransactionId: `opening-${wallet.id}`,
            idempotencyKey,
            payloadHash,
            walletId: wallet.id,
            playerId: wallet.playerId,
            roundId: `opening-round-${wallet.id}`,
            gameId: 'wallet-opening',
            kind: WagerTransactionKind.Opening,
            money: initialMoney,
            createdAt: now,
            allowInternalOpening: true,
          });

          openingTx.markProcessed(undefined, now, wallet.balance);
          const openingEntry = wallet.createOpeningLedgerEntry(openingTx.id);

          txEm.persist(DomainPersistenceMapper.toOrmWagerTransaction(openingTx));
          await txEm.flush();

          if (openingEntry) {
            txEm.persist(DomainPersistenceMapper.toOrmLedgerEntry(openingEntry));

            const balanceChangedEvent = WalletBalanceChanged.from(wallet, openingEntry, {
              correlationId,
              occurredAt: now,
            });
            const processedEvent = WagerTransactionProcessed.from(openingTx, wallet, {
              correlationId,
              occurredAt: now,
            });

            txEm.persist(
              DomainPersistenceMapper.toOrmOutboxMessage(
                OutboxMessage.enqueue(processedEvent),
              ),
            );
            txEm.persist(
              DomainPersistenceMapper.toOrmOutboxMessage(
                OutboxMessage.enqueue(balanceChangedEvent),
              ),
            );
          }

          this.metrics.incrementTransactionStatus(openingTx.status, openingTx.kind);
          await txEm.flush();
        }

        return wallet;
      });

      this.logger.info('wallet.created', {
        correlationId,
        walletId: result.id,
        playerId: result.playerId,
        currency: result.currency,
      });

      return this.toWalletResponse(result);
    } catch (err: unknown) {
      if (err instanceof DuplicateWalletError) {
        throw err;
      }
      const pgCode = (err as { code?: string })?.code;
      const msg = (err as Error)?.message ?? '';
      if (pgCode === '23505' || msg.includes('uq_wallets_player_currency')) {
        throw new DuplicateWalletError(input.playerId, initialMoney.currency);
      }
      throw err;
    }
  }

  async getWallet(walletId: string): Promise<WalletResponseDto> {
    const orm = await this.em.findOne(WalletOrmEntity, { id: walletId });
    if (!orm) {
      throw new EntityNotFoundError('Wallet', walletId);
    }
    const domain = DomainPersistenceMapper.toDomainWallet(orm);
    return this.toWalletResponse(domain);
  }

  async listWallets(filters?: {
    playerId?: string;
    currency?: string;
    status?: string;
    limit?: number;
  }): Promise<WalletResponseDto[]> {
    const where: Record<string, unknown> = {};
    if (filters?.playerId) {
      where.playerId = filters.playerId;
    }
    if (filters?.currency) {
      where.currency = filters.currency.toUpperCase();
    }
    if (filters?.status) {
      where.status = filters.status.toUpperCase();
    }

    const rows = await this.em.find(WalletOrmEntity, where, {
      orderBy: { createdAt: 'DESC' },
      limit: Math.min(200, Math.max(1, filters?.limit ?? 50)),
    });

    return rows.map((row) =>
      this.toWalletResponse(DomainPersistenceMapper.toDomainWallet(row)),
    );
  }

  async updateWallet(
    walletId: string,
    input: { status?: WalletStatus; metadata?: Record<string, unknown> },
  ): Promise<WalletResponseDto> {
    const updated = await this.em.transactional(async (txEm) => {
      const orm = await txEm.findOne(
        WalletOrmEntity,
        { id: walletId },
        { lockMode: LockMode.PESSIMISTIC_WRITE },
      );
      if (!orm) {
        throw new EntityNotFoundError('Wallet', walletId);
      }

      const wallet = DomainPersistenceMapper.toDomainWallet(orm);
      wallet.updateDetails({
        status: input.status,
        metadata: input.metadata,
        now: new Date(),
      });

      DomainPersistenceMapper.toOrmWallet(wallet, orm);
      await txEm.flush();
      return wallet;
    });

    return this.toWalletResponse(updated);
  }

  async closeWallet(walletId: string): Promise<WalletResponseDto> {
    return this.updateWallet(walletId, { status: WalletStatus.Closed });
  }

  async listWalletLedger(params: {
    walletId: string;
    cursor?: string;
    limit?: number;
  }): Promise<LedgerPageResponseDto> {
    const walletOrm = await this.em.findOne(WalletOrmEntity, { id: params.walletId });
    if (!walletOrm) {
      throw new EntityNotFoundError('Wallet', params.walletId);
    }

    const pageSize = Math.min(200, Math.max(1, Number(params.limit ?? 50)));
    let cursorData: LedgerCursorPayload | undefined;
    if (params.cursor && params.cursor.trim().length > 0) {
      cursorData = decodeLedgerCursor(params.cursor.trim());
    }

    const allEntries = await this.em.find(
      WalletLedgerEntryOrmEntity,
      { walletId: params.walletId },
      { orderBy: { createdAt: 'ASC', id: 'ASC' } },
    );

    let filtered = allEntries;
    if (cursorData) {
      const cursorTime = new Date(cursorData.createdAt).getTime();
      filtered = allEntries.filter((e) => {
        const entryTime = new Date(e.createdAt).getTime();
        if (entryTime > cursorTime) {
          return true;
        }
        if (entryTime === cursorTime && e.id > cursorData!.id) {
          return true;
        }
        return false;
      });
    }

    const hasMore = filtered.length > pageSize;
    const slice = filtered.slice(0, pageSize);

    const domainItems = slice.map((orm) =>
      DomainPersistenceMapper.toDomainLedgerEntry(orm),
    );

    const lastItem = domainItems[domainItems.length - 1];
    const nextCursor =
      hasMore && lastItem
        ? encodeLedgerCursor({
            createdAt: lastItem.createdAt.toISOString(),
            id: lastItem.id,
          })
        : null;

    return {
      walletId: params.walletId,
      items: domainItems.map((entry) => ({
        id: entry.id,
        walletId: entry.walletId,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        createdAt: entry.createdAt.toISOString(),
        doubleEntry: entry.toDoubleEntryPosting(),
      })),
      nextCursor,
      hasMore,
    };
  }

  async reconcileWallet(
    walletId: string,
    correlationId: string = randomUUID(),
  ): Promise<ReconciliationResponseDto> {
    const result = await this.em.transactional(async (txEm) => {
      const walletOrm = await txEm.findOne(
        WalletOrmEntity,
        { id: walletId },
        { lockMode: LockMode.PESSIMISTIC_READ },
      );

      if (!walletOrm) {
        throw new EntityNotFoundError('Wallet', walletId);
      }

      const wallet = DomainPersistenceMapper.toDomainWallet(walletOrm);
      const ledgerOrms = await txEm.find(
        WalletLedgerEntryOrmEntity,
        { walletId },
        { orderBy: { createdAt: 'ASC', id: 'ASC' } },
      );

      let calculated = Money.zero(wallet.currency);
      let chainConsistent = true;

      for (const ormEntry of ledgerOrms) {
        const entry = DomainPersistenceMapper.toDomainLedgerEntry(ormEntry);
        if (!entry.isBalanced()) {
          chainConsistent = false;
        }
        if (!entry.balanceBefore.equals(calculated)) {
          chainConsistent = false;
        }
        if (entry.direction === LedgerDirection.Credit) {
          calculated = calculated.add(entry.money);
        } else {
          calculated = calculated.subtract(entry.money);
        }
        if (!entry.balanceAfter.equals(calculated)) {
          chainConsistent = false;
        }
      }

      const difference = wallet.balance.subtract(calculated);
      const consistent = difference.isZero() && chainConsistent;

      return {
        walletId: wallet.id,
        storedBalance: wallet.balance.toJSON(),
        calculatedBalance: calculated.toJSON(),
        difference: difference.toJSON(),
        consistent,
        checkedEntries: ledgerOrms.length,
      };
    });

    if (!result.consistent) {
      this.metrics.incrementReconciliationDivergence();
      this.logger.error('wallet.reconciliation.divergence_detected', {
        correlationId,
        walletId,
        checkedEntries: result.checkedEntries,
        consistent: false,
      });
    } else {
      this.logger.info('wallet.reconciliation.completed', {
        correlationId,
        walletId,
        checkedEntries: result.checkedEntries,
        consistent: true,
      });
    }

    this.realtimeNotifier?.notifyReconciliation(result);

    return result;
  }

  private toWalletResponse(wallet: Wallet): WalletResponseDto {
    return {
      id: wallet.id,
      playerId: wallet.playerId,
      balance: wallet.balance.toJSON(),
      version: wallet.version,
      status: wallet.status,
      metadata: { ...wallet.metadata },
      createdAt: wallet.createdAt.toISOString(),
      updatedAt: wallet.updatedAt.toISOString(),
    };
  }
}
