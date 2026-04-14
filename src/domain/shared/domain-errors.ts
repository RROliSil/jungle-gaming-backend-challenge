export enum FailureCode {
  InsufficientFunds = 'INSUFFICIENT_FUNDS',
  ReversalInsufficientFunds = 'REVERSAL_INSUFFICIENT_FUNDS',
  CurrencyMismatch = 'CURRENCY_MISMATCH',
  ReferenceNotFound = 'REFERENCE_NOT_FOUND',
  ReferenceKindInvalid = 'REFERENCE_KIND_INVALID',
  ReferenceContextMismatch = 'REFERENCE_CONTEXT_MISMATCH',
  ReferenceNotProcessed = 'REFERENCE_NOT_PROCESSED',
  ReferenceAlreadyReversed = 'REFERENCE_ALREADY_REVERSED',
  ReferenceAmountMismatch = 'REFERENCE_AMOUNT_MISMATCH',
  WalletNotActive = 'WALLET_NOT_ACTIVE',
  InfrastructureFailure = 'INFRASTRUCTURE_FAILURE',
}

export class DomainError extends Error {
  public readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = this.constructor.name;
    this.code = code;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class InvalidMoneyError extends DomainError {
  constructor(message: string) {
    super('INVALID_MONEY_FORMAT', message);
  }
}

export class CurrencyMismatchError extends DomainError {
  constructor(expected: string, actual: string) {
    super(
      FailureCode.CurrencyMismatch,
      `Currency mismatch: expected ${expected}, received ${actual}`,
    );
  }
}

export class InsufficientFundsError extends DomainError {
  public readonly failureCode: FailureCode;

  constructor(
    walletId: string,
    available: string,
    requested: string,
    isReversal = false,
  ) {
    const code = isReversal
      ? FailureCode.ReversalInsufficientFunds
      : FailureCode.InsufficientFunds;
    super(
      code,
      `Insufficient funds in wallet ${walletId}: available=${available}, requested=${requested}`,
    );
    this.failureCode = code;
  }
}

export class InvalidTransactionStateError extends DomainError {
  constructor(transactionId: string, currentStatus: string, targetAction: string) {
    super(
      'INVALID_TRANSACTION_STATE',
      `Cannot perform '${targetAction}' on transaction ${transactionId} in state ${currentStatus}`,
    );
  }
}

export class InvalidTransactionPropsError extends DomainError {
  constructor(code: string, message: string) {
    super(code, message);
  }
}

export class LedgerImbalanceError extends DomainError {
  constructor(message: string) {
    super('LEDGER_IMBALANCE', message);
  }
}

export class IdempotencyConflictError extends DomainError {
  constructor(idempotencyKey: string) {
    super(
      'IDEMPOTENCY_PAYLOAD_MISMATCH',
      `Idempotency key '${idempotencyKey}' was already used with a different request payload`,
    );
  }
}

export class DuplicateWalletError extends DomainError {
  constructor(playerId: string, currency: string) {
    super(
      'WALLET_ALREADY_EXISTS',
      `Wallet already exists for playerId=${playerId} and currency=${currency}`,
    );
  }
}

export class EntityNotFoundError extends DomainError {
  constructor(entityName: string, identifier: string) {
    super('ENTITY_NOT_FOUND', `${entityName} not found: ${identifier}`);
  }
}
