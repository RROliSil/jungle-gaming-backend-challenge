import Decimal from 'decimal.js';
import { CurrencyMismatchError, InvalidMoneyError } from './domain-errors';

export interface MoneyProps {
  amount: string;   // decimal string, ex.: "25.00"
  currency: string; // ISO-4217
}

const CURRENCY_REGEX = /^[A-Z]{3}$/;
// Strict fixed-scale 2 decimal places or signed for internal representation
const INPUT_DECIMAL_REGEX = /^\d+\.\d{2}$/;
const INTERNAL_DECIMAL_REGEX = /^-?\d+\.\d{2}$/;

export class Money {
  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ) {
    Object.freeze(this);
  }

  /**
   * Creates a Money instance from an external/input contract.
   * Rejects NaN, Infinity, scientific notation, empty string, >2 decimal places,
   * missing 2 decimal scale, or negative values in input contracts.
   */
  static from(props: MoneyProps): Money {
    if (!props || typeof props !== 'object') {
      throw new InvalidMoneyError('MoneyProps must be a valid object');
    }

    const { amount, currency } = props;

    if (typeof currency !== 'string' || !CURRENCY_REGEX.test(currency)) {
      throw new InvalidMoneyError(
        `Invalid ISO-4217 currency code: '${String(currency)}'`,
      );
    }

    if (typeof amount !== 'string' || amount.trim() !== amount || amount.length === 0) {
      throw new InvalidMoneyError(
        `Amount must be a non-empty decimal string with scale 2, received: '${String(amount)}'`,
      );
    }

    if (amount.startsWith('-')) {
      throw new InvalidMoneyError(
        `Negative amount is not allowed in input contracts: '${amount}'`,
      );
    }

    if (!INPUT_DECIMAL_REGEX.test(amount)) {
      throw new InvalidMoneyError(
        `Amount must be a decimal string with exact scale of 2 decimal places (e.g. "25.00"), received: '${amount}'`,
      );
    }

    const decimalValue = new Decimal(amount);
    if (!decimalValue.isFinite() || decimalValue.isNaN()) {
      throw new InvalidMoneyError(`Amount is not finite: '${amount}'`);
    }

    return new Money(decimalValue, currency);
  }

  /**
   * Rehydrates a persisted or internal Money (allows negative only when produced by negate(),
   * still enforces exact 2 decimal scale and ISO-4217 currency).
   */
  static rehydrate(props: MoneyProps): Money {
    if (!props || typeof props.currency !== 'string' || !CURRENCY_REGEX.test(props.currency)) {
      throw new InvalidMoneyError(`Invalid currency on rehydrate: '${String(props?.currency)}'`);
    }
    if (typeof props.amount !== 'string' || !INTERNAL_DECIMAL_REGEX.test(props.amount)) {
      throw new InvalidMoneyError(`Invalid amount on rehydrate: '${String(props?.amount)}'`);
    }
    return new Money(new Decimal(props.amount), props.currency);
  }

  static zero(currency: string): Money {
    if (typeof currency !== 'string' || !CURRENCY_REGEX.test(currency)) {
      throw new InvalidMoneyError(`Invalid ISO-4217 currency code: '${String(currency)}'`);
    }
    return new Money(new Decimal('0.00'), currency);
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.plus(other.value), this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.minus(other.value), this.currency);
  }

  negate(): Money {
    const negated = this.value.isZero() ? new Decimal('0.00') : this.value.negated();
    return new Money(negated, this.currency);
  }

  isZero(): boolean {
    return this.value.isZero();
  }

  isPositive(): boolean {
    return this.value.greaterThan(0);
  }

  isNegative(): boolean {
    return this.value.lessThan(0);
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.lessThan(other.value);
  }

  equals(other: Money): boolean {
    if (!(other instanceof Money)) {
      return false;
    }
    return this.currency === other.currency && this.value.equals(other.value);
  }

  get amount(): string {
    return this.value.toFixed(2);
  }

  toJSON(): MoneyProps {
    return {
      amount: this.value.toFixed(2),
      currency: this.currency,
    };
  }

  toString(): string {
    return `${this.value.toFixed(2)} ${this.currency}`;
  }

  private assertSameCurrency(other: Money): void {
    if (!other || !(other instanceof Money)) {
      throw new InvalidMoneyError('Expected a valid Money instance');
    }
    if (this.currency !== other.currency) {
      throw new CurrencyMismatchError(this.currency, other.currency);
    }
  }
}
