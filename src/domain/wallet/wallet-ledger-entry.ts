import { randomUUID } from 'crypto';
import {
  CurrencyMismatchError,
  InvalidMoneyError,
  LedgerImbalanceError,
} from '../shared/domain-errors';
import { Money } from '../shared/money';

export enum LedgerDirection {
  Debit = 'DEBIT',
  Credit = 'CREDIT',
}

export interface CreateLedgerEntryProps {
  id?: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt?: Date;
}

export interface LedgerEntryState {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt: Date;
}

export interface DoubleEntryPosting {
  debitAccount: string;
  creditAccount: string;
  amount: string;
  currency: string;
}

export class WalletLedgerEntry {
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly createdAt: Date,
  ) {
    Object.freeze(this);
  }

  static create(props: CreateLedgerEntryProps): WalletLedgerEntry {
    if (!props.walletId || props.walletId.trim().length === 0) {
      throw new LedgerImbalanceError('walletId is required for WalletLedgerEntry');
    }
    if (!props.transactionId || props.transactionId.trim().length === 0) {
      throw new LedgerImbalanceError('transactionId is required for WalletLedgerEntry');
    }
    if (
      props.direction !== LedgerDirection.Debit &&
      props.direction !== LedgerDirection.Credit
    ) {
      throw new LedgerImbalanceError(`Invalid ledger direction: '${String(props.direction)}'`);
    }

    if (!props.money.isPositive()) {
      throw new InvalidMoneyError(
        `Ledger entry money must be strictly positive (> 0.00), received: ${props.money.toString()}`,
      );
    }

    if (
      props.money.currency !== props.balanceBefore.currency ||
      props.money.currency !== props.balanceAfter.currency
    ) {
      throw new CurrencyMismatchError(
        props.balanceBefore.currency,
        props.money.currency,
      );
    }

    if (props.balanceBefore.isNegative() || props.balanceAfter.isNegative()) {
      throw new LedgerImbalanceError(
        'Ledger entry balanceBefore and balanceAfter cannot be negative',
      );
    }

    const entry = new WalletLedgerEntry(
      props.id ?? randomUUID(),
      props.walletId,
      props.transactionId,
      props.direction,
      props.money,
      props.balanceBefore,
      props.balanceAfter,
      props.createdAt ? new Date(props.createdAt.getTime()) : new Date(),
    );

    if (!entry.isBalanced()) {
      throw new LedgerImbalanceError(
        `Unbalanced ledger entry: direction=${props.direction}, before=${props.balanceBefore.amount}, money=${props.money.amount}, after=${props.balanceAfter.amount}`,
      );
    }

    return entry;
  }

  static rehydrate(state: LedgerEntryState): WalletLedgerEntry {
    return new WalletLedgerEntry(
      state.id,
      state.walletId,
      state.transactionId,
      state.direction,
      state.money,
      state.balanceBefore,
      state.balanceAfter,
      new Date(state.createdAt.getTime()),
    );
  }

  /** balanceBefore ± money === balanceAfter. Verificada na factory. */
  isBalanced(): boolean {
    if (
      this.money.currency !== this.balanceBefore.currency ||
      this.money.currency !== this.balanceAfter.currency
    ) {
      return false;
    }

    if (this.direction === LedgerDirection.Credit) {
      return this.balanceBefore.add(this.money).equals(this.balanceAfter);
    }

    if (this.direction === LedgerDirection.Debit) {
      return this.balanceBefore.subtract(this.money).equals(this.balanceAfter);
    }

    return false;
  }

  /**
   * Optional Double-Entry Bookkeeping projection (Section 6.4 differential):
   * Maps every single-wallet ledger entry onto balanced debit/credit accounts
   * between the player liability account and the house clearing account.
   */
  toDoubleEntryPosting(): DoubleEntryPosting {
    const playerAccount = `LIABILITY:PLAYER_WALLET:${this.walletId}`;
    const houseAccount = `EQUITY:HOUSE_CLEARING:${this.money.currency}`;

    if (this.direction === LedgerDirection.Credit) {
      return {
        debitAccount: houseAccount,
        creditAccount: playerAccount,
        amount: this.money.amount,
        currency: this.money.currency,
      };
    }

    return {
      debitAccount: playerAccount,
      creditAccount: houseAccount,
      amount: this.money.amount,
      currency: this.money.currency,
    };
  }
}
