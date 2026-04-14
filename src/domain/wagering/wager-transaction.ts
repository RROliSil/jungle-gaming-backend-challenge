import { randomUUID } from 'crypto';
import {
  FailureCode,
  InvalidMoneyError,
  InvalidTransactionPropsError,
  InvalidTransactionStateError,
} from '../shared/domain-errors';
import { Money } from '../shared/money';
import { LedgerDirection } from '../wallet/wallet-ledger-entry';

export enum WagerTransactionKind {
  Opening = 'OPENING',   // interno: crédito de abertura da wallet
  Bet = 'BET',
  Win = 'WIN',
  Loss = 'LOSS',
  Refund = 'REFUND',
  Rollback = 'ROLLBACK',
}

export enum WagerTransactionStatus {
  Pending = 'PENDING',                   // aceita, ainda não aplicada
  PendingReference = 'PENDING_REFERENCE', // aguardando a transação referenciada
  Processed = 'PROCESSED',               // aplicada (terminal)
  Rejected = 'REJECTED',                 // violação de regra de negócio (terminal)
  Failed = 'FAILED',                     // erro permanente de infraestrutura (terminal, auditável)
}

export interface CreateWagerTransactionProps {
  id?: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt?: Date;
  allowInternalOpening?: boolean;
}

export interface WagerTransactionState {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt: Date;
  status: WagerTransactionStatus;
  referenceTransactionId?: string;
  failureCode?: FailureCode;
  processedAt?: Date;
  observedBalance?: Money;
  retryCount?: number;
  nextRetryAt?: Date;
}

export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    /** id no provedor — não o id interno */
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: FailureCode,
    private _processedAt?: Date,
    private _observedBalance?: Money,
    private _retryCount = 0,
    private _nextRetryAt?: Date,
  ) {}

  /** Nasce em PENDING. Valida a exigência de referência por kind. */
  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (!props.providerId || props.providerId.trim().length === 0) {
      throw new InvalidTransactionPropsError('INVALID_PROVIDER_ID', 'providerId is required');
    }
    if (!props.externalTransactionId || props.externalTransactionId.trim().length === 0) {
      throw new InvalidTransactionPropsError(
        'INVALID_EXTERNAL_TX_ID',
        'externalTransactionId is required',
      );
    }
    if (!props.idempotencyKey || props.idempotencyKey.trim().length === 0) {
      throw new InvalidTransactionPropsError(
        'MISSING_IDEMPOTENCY_KEY',
        'idempotencyKey is required',
      );
    }
    if (!props.payloadHash || props.payloadHash.trim().length === 0) {
      throw new InvalidTransactionPropsError('INVALID_PAYLOAD_HASH', 'payloadHash is required');
    }
    if (!props.walletId || props.walletId.trim().length === 0) {
      throw new InvalidTransactionPropsError('INVALID_WALLET_ID', 'walletId is required');
    }
    if (!props.playerId || props.playerId.trim().length === 0) {
      throw new InvalidTransactionPropsError('INVALID_PLAYER_ID', 'playerId is required');
    }
    if (!props.roundId || props.roundId.trim().length === 0) {
      throw new InvalidTransactionPropsError('INVALID_ROUND_ID', 'roundId is required');
    }
    if (!props.gameId || props.gameId.trim().length === 0) {
      throw new InvalidTransactionPropsError('INVALID_GAME_ID', 'gameId is required');
    }

    if (
      props.kind === WagerTransactionKind.Opening &&
      !props.allowInternalOpening
    ) {
      throw new InvalidTransactionPropsError(
        'FORBIDDEN_OPENING_KIND',
        'OPENING transaction kind is internal only and cannot be submitted via API or queue',
      );
    }

    if (!Object.values(WagerTransactionKind).includes(props.kind)) {
      throw new InvalidTransactionPropsError(
        'INVALID_TRANSACTION_KIND',
        `Unsupported transaction kind: '${String(props.kind)}'`,
      );
    }

    if (props.money.isNegative()) {
      throw new InvalidMoneyError('Transaction money cannot be negative');
    }

    if (
      props.kind !== WagerTransactionKind.Loss &&
      !props.money.isPositive()
    ) {
      throw new InvalidMoneyError(
        `Transaction of kind ${props.kind} must have a strictly positive amount`,
      );
    }

    const requiresRef =
      props.kind === WagerTransactionKind.Refund ||
      props.kind === WagerTransactionKind.Rollback;

    if (
      requiresRef &&
      (!props.referenceExternalTransactionId ||
        props.referenceExternalTransactionId.trim().length === 0)
    ) {
      throw new InvalidTransactionPropsError(
        'MISSING_REFERENCE_ID',
        `${props.kind} requires referenceExternalTransactionId`,
      );
    }

    const now = props.createdAt ? new Date(props.createdAt.getTime()) : new Date();

    return new WagerTransaction(
      props.id ?? randomUUID(),
      props.providerId,
      props.externalTransactionId,
      props.idempotencyKey,
      props.payloadHash,
      props.walletId,
      props.playerId,
      props.roundId,
      props.gameId,
      props.kind,
      props.money,
      props.referenceExternalTransactionId,
      now,
      WagerTransactionStatus.Pending,
    );
  }

  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      state.id,
      state.providerId,
      state.externalTransactionId,
      state.idempotencyKey,
      state.payloadHash,
      state.walletId,
      state.playerId,
      state.roundId,
      state.gameId,
      state.kind,
      state.money,
      state.referenceExternalTransactionId,
      new Date(state.createdAt.getTime()),
      state.status,
      state.referenceTransactionId,
      state.failureCode,
      state.processedAt ? new Date(state.processedAt.getTime()) : undefined,
      state.observedBalance,
      state.retryCount ?? 0,
      state.nextRetryAt ? new Date(state.nextRetryAt.getTime()) : undefined,
    );
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }

  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }

  get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  get observedBalance(): Money | undefined {
    return this._observedBalance;
  }

  get retryCount(): number {
    return this._retryCount;
  }

  get nextRetryAt(): Date | undefined {
    return this._nextRetryAt;
  }

  // ---- transições (lançam InvalidTransactionStateError se o estado atual for terminal)
  markProcessed(
    referenceTransactionId: string | undefined,
    at: Date,
    observedBalance?: Money,
  ): void {
    this.assertNotTerminal('markProcessed');
    this._status = WagerTransactionStatus.Processed;
    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = new Date(at.getTime());
    this._failureCode = undefined;
    if (observedBalance) {
      this._observedBalance = observedBalance;
    }
  }

  markPendingReference(nextRetryAt?: Date): void {
    this.assertNotTerminal('markPendingReference');
    this._status = WagerTransactionStatus.PendingReference;
    if (nextRetryAt) {
      this._nextRetryAt = new Date(nextRetryAt.getTime());
    } else if (!this._nextRetryAt) {
      this._nextRetryAt = new Date(Date.now() + 200);
    }
  }

  scheduleReferenceRetry(now: Date, baseDelayMs = 200): void {
    this.assertNotTerminal('scheduleReferenceRetry');
    this._status = WagerTransactionStatus.PendingReference;
    this._retryCount += 1;
    const delayMs = baseDelayMs * Math.pow(2, Math.max(0, this._retryCount - 1));
    this._nextRetryAt = new Date(now.getTime() + delayMs);
  }

  reject(code: FailureCode, at: Date = new Date(), observedBalance?: Money): void {
    this.assertNotTerminal('reject');
    this._status = WagerTransactionStatus.Rejected;
    this._failureCode = code;
    this._processedAt = new Date(at.getTime());
    if (observedBalance) {
      this._observedBalance = observedBalance;
    }
  }

  fail(code: FailureCode, at: Date = new Date(), observedBalance?: Money): void {
    this.assertNotTerminal('fail');
    this._status = WagerTransactionStatus.Failed;
    this._failureCode = code;
    this._processedAt = new Date(at.getTime());
    if (observedBalance) {
      this._observedBalance = observedBalance;
    }
  }

  recordObservedBalance(balance: Money): void {
    this._observedBalance = balance;
  }

  // ---- consultas de domínio
  isTerminal(): boolean {
    return (
      this._status === WagerTransactionStatus.Processed ||
      this._status === WagerTransactionStatus.Rejected ||
      this._status === WagerTransactionStatus.Failed
    );
  }

  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }

  requiresReference(): boolean {
    return (
      this.kind === WagerTransactionKind.Refund ||
      this.kind === WagerTransactionKind.Rollback
    );
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Opening:
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
        return LedgerDirection.Credit;

      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;

      case WagerTransactionKind.Rollback: {
        if (!reference) {
          throw new InvalidTransactionPropsError(
            FailureCode.ReferenceNotFound,
            'ROLLBACK requires a resolved reference transaction to determine ledger direction',
          );
        }
        if (reference.kind === WagerTransactionKind.Bet) {
          return LedgerDirection.Credit;
        }
        if (
          reference.kind === WagerTransactionKind.Win ||
          reference.kind === WagerTransactionKind.Refund
        ) {
          return LedgerDirection.Debit;
        }
        throw new InvalidTransactionPropsError(
          FailureCode.ReferenceKindInvalid,
          `ROLLBACK cannot reference a transaction of kind ${reference.kind}`,
        );
      }

      case WagerTransactionKind.Loss:
      default:
        throw new InvalidTransactionPropsError(
          'LOSS_NO_LEDGER_DIRECTION',
          `Transaction of kind ${this.kind} does not produce a ledger entry`,
        );
    }
  }

  private assertNotTerminal(action: string): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(this.id, this._status, action);
    }
  }
}
