import {
  CurrencyMismatchError,
  DomainError,
  FailureCode,
  InsufficientFundsError,
  InvalidMoneyError,
} from '../shared/domain-errors';
import { Money } from '../shared/money';
import { LedgerDirection, WalletLedgerEntry } from './wallet-ledger-entry';

export enum WalletStatus {
  Active = 'ACTIVE',
  Frozen = 'FROZEN',
  Closed = 'CLOSED',
}

export interface OpenWalletProps {
  id: string;
  playerId: string;
  initialBalance: Money;
  status?: WalletStatus;
  metadata?: Record<string, unknown>;
  createdAt?: Date;
}

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  status?: WalletStatus;
  metadata?: Record<string, unknown>;
  createdAt: Date;
  updatedAt: Date;
}

export interface WalletMovementOptions {
  transactionId: string;
  amount: Money;
  entryId?: string;
  occurredAt?: Date;
  isReversal?: boolean;
}

export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
    private _status: WalletStatus = WalletStatus.Active,
    private _metadata: Record<string, unknown> = {},
  ) {}

  static open(props: OpenWalletProps): Wallet {
    if (!props.id || props.id.trim().length === 0) {
      throw new DomainError('INVALID_WALLET_ID', 'Wallet id is required');
    }
    if (!props.playerId || props.playerId.trim().length === 0) {
      throw new DomainError('INVALID_PLAYER_ID', 'Wallet playerId is required');
    }
    if (props.initialBalance.isNegative()) {
      throw new InvalidMoneyError('Initial wallet balance cannot be negative');
    }

    const now = props.createdAt ? new Date(props.createdAt.getTime()) : new Date();
    return new Wallet(
      props.id,
      props.playerId,
      props.initialBalance.currency,
      props.initialBalance,
      1, // version starts at 1 after creation
      now,
      now,
      props.status ?? WalletStatus.Active,
      props.metadata ? { ...props.metadata } : {},
    );
  }

  /** Reconstrução a partir da persistência — não revalida transições. */
  static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      state.balance,
      state.version,
      new Date(state.createdAt.getTime()),
      new Date(state.updatedAt.getTime()),
      state.status ?? WalletStatus.Active,
      state.metadata ? { ...state.metadata } : {},
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  get status(): WalletStatus {
    return this._status;
  }

  get metadata(): Readonly<Record<string, unknown>> {
    return { ...this._metadata };
  }

  /**
   * Produces the initial OPENING ledger entry when a wallet is opened with initialBalance > 0.
   * Does not increment version beyond 1 (since version starts at 1 upon creation).
   */
  createOpeningLedgerEntry(transactionId: string, entryId?: string): WalletLedgerEntry | null {
    if (this._balance.isZero()) {
      return null;
    }

    return WalletLedgerEntry.create({
      id: entryId,
      walletId: this.id,
      transactionId,
      direction: LedgerDirection.Credit,
      money: this._balance,
      balanceBefore: Money.zero(this.currency),
      balanceAfter: this._balance,
      createdAt: this.createdAt,
    });
  }

  /**
   * Applies a debit movement, maintaining balance and ledger consistency.
   * Increments version ONLY when balance changes.
   */
  debit(options: WalletMovementOptions): WalletLedgerEntry {
    this.assertActive();
    this.assertSameCurrency(options.amount);

    if (!options.amount.isPositive()) {
      throw new InvalidMoneyError('Debit amount must be strictly positive');
    }

    if (this._balance.isLessThan(options.amount)) {
      throw new InsufficientFundsError(
        this.id,
        this._balance.amount,
        options.amount.amount,
        Boolean(options.isReversal),
      );
    }

    const balanceBefore = this._balance;
    const balanceAfter = this._balance.subtract(options.amount);
    const now = options.occurredAt ? new Date(options.occurredAt.getTime()) : new Date();

    const entry = WalletLedgerEntry.create({
      id: options.entryId,
      walletId: this.id,
      transactionId: options.transactionId,
      direction: LedgerDirection.Debit,
      money: options.amount,
      balanceBefore,
      balanceAfter,
      createdAt: now,
    });

    this._balance = balanceAfter;
    this._version += 1;
    this._updatedAt = now;

    return entry;
  }

  /**
   * Applies a credit movement, maintaining balance and ledger consistency.
   * Increments version ONLY when balance changes.
   */
  credit(options: WalletMovementOptions): WalletLedgerEntry {
    this.assertActive();
    this.assertSameCurrency(options.amount);

    if (!options.amount.isPositive()) {
      throw new InvalidMoneyError('Credit amount must be strictly positive');
    }

    const balanceBefore = this._balance;
    const balanceAfter = this._balance.add(options.amount);
    const now = options.occurredAt ? new Date(options.occurredAt.getTime()) : new Date();

    const entry = WalletLedgerEntry.create({
      id: options.entryId,
      walletId: this.id,
      transactionId: options.transactionId,
      direction: LedgerDirection.Credit,
      money: options.amount,
      balanceBefore,
      balanceAfter,
      createdAt: now,
    });

    this._balance = balanceAfter;
    this._version += 1;
    this._updatedAt = now;

    return entry;
  }

  /**
   * Administrative metadata/status update for CRUD operations.
   * Never alters balance or version (version only increments when balance changes).
   */
  updateDetails(props: {
    status?: WalletStatus;
    metadata?: Record<string, unknown>;
    now?: Date;
  }): void {
    if (this._status === WalletStatus.Closed) {
      throw new DomainError(
        FailureCode.WalletNotActive,
        `Cannot update closed wallet ${this.id}`,
      );
    }

    if (props.status === WalletStatus.Closed && !this._balance.isZero()) {
      throw new DomainError(
        'WALLET_NON_ZERO_BALANCE',
        `Cannot close wallet ${this.id} while balance is non-zero (${this._balance.toString()})`,
      );
    }

    if (props.status !== undefined) {
      this._status = props.status;
    }

    if (props.metadata !== undefined) {
      this._metadata = { ...this._metadata, ...props.metadata };
    }

    this._updatedAt = props.now ? new Date(props.now.getTime()) : new Date();
  }

  close(now?: Date): void {
    this.updateDetails({ status: WalletStatus.Closed, now });
  }

  private assertActive(): void {
    if (this._status !== WalletStatus.Active) {
      throw new DomainError(
        FailureCode.WalletNotActive,
        `Wallet ${this.id} is not active (current status: ${this._status})`,
      );
    }
  }

  private assertSameCurrency(money: Money): void {
    if (this.currency !== money.currency) {
      throw new CurrencyMismatchError(this.currency, money.currency);
    }
  }
}
