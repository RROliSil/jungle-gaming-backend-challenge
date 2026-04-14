import { describe, expect, it } from 'bun:test';
import { Game } from '../../src/domain/catalog/game';
import { GameProvider, ProviderStatus } from '../../src/domain/catalog/game-provider';
import { InboxMessage } from '../../src/domain/messaging/inbox-message';
import { OutboxMessage } from '../../src/domain/messaging/outbox-message';
import {
  computeCanonicalPayloadHash,
  computeWagerPayloadHash,
  toCanonicalJson,
} from '../../src/domain/shared/canonical-json';
import {
  CurrencyMismatchError,
  FailureCode,
  InsufficientFundsError,
  InvalidMoneyError,
  InvalidTransactionPropsError,
  InvalidTransactionStateError,
  LedgerImbalanceError,
} from '../../src/domain/shared/domain-errors';
import { Money } from '../../src/domain/shared/money';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../src/domain/wagering/wager-transaction';
import {
  WagerTransactionProcessed,
  WalletBalanceChanged,
} from '../../src/domain/wagering/wagering-events';
import { Wallet, WalletStatus } from '../../src/domain/wallet/wallet';
import {
  LedgerDirection,
  WalletLedgerEntry,
} from '../../src/domain/wallet/wallet-ledger-entry';

describe('Unit — Money (Section 6.1)', () => {
  it('creates immutable Money from valid decimal string with fixed scale of 2', () => {
    const m1 = Money.from({ amount: '25.00', currency: 'BRL' });
    const m2 = Money.from({ amount: '10.50', currency: 'BRL' });

    expect(m1.amount).toBe('25.00');
    expect(m1.currency).toBe('BRL');
    expect(m1.isPositive()).toBe(true);
    expect(m1.isZero()).toBe(false);
    expect(m1.isNegative()).toBe(false);
    expect(m1.toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
    expect(m1.toString()).toBe('25.00 BRL');

    const sum = m1.add(m2);
    expect(sum.toJSON()).toEqual({ amount: '35.50', currency: 'BRL' });
    // Original instances remain untouched (immutability)
    expect(m1.amount).toBe('25.00');
    expect(m2.amount).toBe('10.50');

    const diff = m1.subtract(m2);
    expect(diff.toJSON()).toEqual({ amount: '14.50', currency: 'BRL' });

    const negated = m1.negate();
    expect(negated.isNegative()).toBe(true);
    expect(negated.toJSON()).toEqual({ amount: '-25.00', currency: 'BRL' });

    const zero = Money.zero('BRL');
    expect(zero.isZero()).toBe(true);
    expect(zero.negate().toJSON()).toEqual({ amount: '0.00', currency: 'BRL' });
    expect(m2.isLessThan(m1)).toBe(true);
    expect(m1.equals(Money.from({ amount: '25.00', currency: 'BRL' }))).toBe(true);
  });

  it('rejects invalid Money inputs: NaN, Infinity, scientific notation, empty string, >2 decimals, <2 decimals, negative input', () => {
    const invalidAmounts = [
      '',
      '   ',
      'NaN',
      'Infinity',
      '-Infinity',
      '1e5',
      '2.5e2',
      '25',
      '25.0',
      '25.000',
      '25.123',
      '-10.00',
      '-0.01',
      'abc',
      '25,00',
      ' 25.00 ',
    ];

    for (const amount of invalidAmounts) {
      expect(() => Money.from({ amount, currency: 'BRL' })).toThrow(InvalidMoneyError);
    }

    const invalidCurrencies = ['', 'BR', 'BRLX', 'brl', '123', 'B$L'];
    for (const currency of invalidCurrencies) {
      expect(() => Money.from({ amount: '10.00', currency })).toThrow(InvalidMoneyError);
    }
  });

  it('throws CurrencyMismatchError when operating on different currencies', () => {
    const brl = Money.from({ amount: '50.00', currency: 'BRL' });
    const usd = Money.from({ amount: '50.00', currency: 'USD' });

    expect(() => brl.add(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl.subtract(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl.isLessThan(usd)).toThrow(CurrencyMismatchError);
    expect(brl.equals(usd)).toBe(false);
  });
});

describe('Unit — Wallet & WalletLedgerEntry Invariants (Sections 6.2 & 6.4)', () => {
  it('opens Wallet with version 1 and increments version ONLY when balance changes', () => {
    const wallet = Wallet.open({
      id: '0192f291-27dd-7d3f-8071-5f8685deef37',
      playerId: '0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1',
      initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
    });

    expect(wallet.version).toBe(1);
    expect(wallet.balance.toJSON()).toEqual({ amount: '100.00', currency: 'BRL' });

    const openingEntry = wallet.createOpeningLedgerEntry('tx-opening-1');
    expect(openingEntry).not.toBeNull();
    expect(openingEntry!.isBalanced()).toBe(true);
    expect(openingEntry!.direction).toBe(LedgerDirection.Credit);
    expect(openingEntry!.balanceBefore.amount).toBe('0.00');
    expect(openingEntry!.balanceAfter.amount).toBe('100.00');
    // Version remains 1 after opening entry
    expect(wallet.version).toBe(1);

    // Debit 25.00 BRL -> balance 75.00 BRL, version 2
    const debitEntry = wallet.debit({
      transactionId: 'tx-bet-1',
      amount: Money.from({ amount: '25.00', currency: 'BRL' }),
    });
    expect(wallet.balance.amount).toBe('75.00');
    expect(wallet.version).toBe(2);
    expect(debitEntry.isBalanced()).toBe(true);
    expect(debitEntry.balanceBefore.amount).toBe('100.00');
    expect(debitEntry.balanceAfter.amount).toBe('75.00');

    // Credit 40.00 BRL -> balance 115.00 BRL, version 3
    const creditEntry = wallet.credit({
      transactionId: 'tx-win-1',
      amount: Money.from({ amount: '40.00', currency: 'BRL' }),
    });
    expect(wallet.balance.amount).toBe('115.00');
    expect(wallet.version).toBe(3);
    expect(creditEntry.isBalanced()).toBe(true);
    expect(creditEntry.balanceBefore.amount).toBe('75.00');
    expect(creditEntry.balanceAfter.amount).toBe('115.00');

    // Administrative metadata update does NOT increment version
    wallet.updateDetails({ metadata: { vipTier: 'gold' } });
    expect(wallet.version).toBe(3);
  });

  it('distinguishes INSUFFICIENT_FUNDS (bet) from REVERSAL_INSUFFICIENT_FUNDS (reversal)', () => {
    const wallet = Wallet.open({
      id: 'wallet-1',
      playerId: 'player-1',
      initialBalance: Money.from({ amount: '20.00', currency: 'BRL' }),
    });

    try {
      wallet.debit({
        transactionId: 'tx-bet-too-high',
        amount: Money.from({ amount: '50.00', currency: 'BRL' }),
        isReversal: false,
      });
      expect.unreachable('Should have thrown InsufficientFundsError');
    } catch (err) {
      expect(err).toBeInstanceOf(InsufficientFundsError);
      expect((err as InsufficientFundsError).failureCode).toBe(
        FailureCode.InsufficientFunds,
      );
    }

    try {
      wallet.debit({
        transactionId: 'tx-rollback-win-too-high',
        amount: Money.from({ amount: '50.00', currency: 'BRL' }),
        isReversal: true,
      });
      expect.unreachable('Should have thrown InsufficientFundsError for reversal');
    } catch (err) {
      expect(err).toBeInstanceOf(InsufficientFundsError);
      expect((err as InsufficientFundsError).failureCode).toBe(
        FailureCode.ReversalInsufficientFunds,
      );
    }
  });

  it('enforces WalletLedgerEntry arithmetic and structural immutability', () => {
    expect(() =>
      WalletLedgerEntry.create({
        walletId: 'wallet-1',
        transactionId: 'tx-1',
        direction: LedgerDirection.Debit,
        money: Money.from({ amount: '30.00', currency: 'BRL' }),
        balanceBefore: Money.from({ amount: '100.00', currency: 'BRL' }),
        balanceAfter: Money.from({ amount: '80.00', currency: 'BRL' }), // Wrong math!
      }),
    ).toThrow(LedgerImbalanceError);

    const validEntry = WalletLedgerEntry.create({
      walletId: 'wallet-1',
      transactionId: 'tx-1',
      direction: LedgerDirection.Debit,
      money: Money.from({ amount: '30.00', currency: 'BRL' }),
      balanceBefore: Money.from({ amount: '100.00', currency: 'BRL' }),
      balanceAfter: Money.from({ amount: '70.00', currency: 'BRL' }),
    });

    expect(validEntry.isBalanced()).toBe(true);
    expect(Object.isFrozen(validEntry)).toBe(true);
    expect(validEntry.toDoubleEntryPosting()).toEqual({
      debitAccount: 'LIABILITY:PLAYER_WALLET:wallet-1',
      creditAccount: 'EQUITY:HOUSE_CLEARING:BRL',
      amount: '30.00',
      currency: 'BRL',
    });
  });
});

describe('Unit — WagerTransaction Rules: BET, WIN, LOSS, REFUND, ROLLBACK (Sections 6.3 & 7)', () => {
  const baseProps = {
    providerId: 'provider-a',
    externalTransactionId: 'ext-1',
    idempotencyKey: 'provider-a:ext-1',
    payloadHash: 'hash-123',
    walletId: 'wallet-1',
    playerId: 'player-1',
    roundId: 'round-1',
    gameId: 'fortune-chimp',
    money: Money.from({ amount: '25.00', currency: 'BRL' }),
  };

  it('forbids external submission of OPENING transaction kind', () => {
    expect(() =>
      WagerTransaction.create({
        ...baseProps,
        kind: WagerTransactionKind.Opening,
      }),
    ).toThrow(InvalidTransactionPropsError);
  });

  it('enforces referenceExternalTransactionId for REFUND and ROLLBACK', () => {
    expect(() =>
      WagerTransaction.create({
        ...baseProps,
        kind: WagerTransactionKind.Refund,
      }),
    ).toThrow(InvalidTransactionPropsError);

    expect(() =>
      WagerTransaction.create({
        ...baseProps,
        kind: WagerTransactionKind.Rollback,
      }),
    ).toThrow(InvalidTransactionPropsError);

    const refundTx = WagerTransaction.create({
      ...baseProps,
      kind: WagerTransactionKind.Refund,
      referenceExternalTransactionId: 'bet-ext-1',
    });
    expect(refundTx.requiresReference()).toBe(true);
    expect(refundTx.affectsBalance()).toBe(true);
    expect(refundTx.status).toBe(WagerTransactionStatus.Pending);
  });

  it('computes ledgerDirectionFor correctly for BET, WIN, LOSS, REFUND, and ROLLBACK', () => {
    const betTx = WagerTransaction.create({
      ...baseProps,
      kind: WagerTransactionKind.Bet,
    });
    const winTx = WagerTransaction.create({
      ...baseProps,
      externalTransactionId: 'ext-win',
      kind: WagerTransactionKind.Win,
    });
    const lossTx = WagerTransaction.create({
      ...baseProps,
      externalTransactionId: 'ext-loss',
      kind: WagerTransactionKind.Loss,
      money: Money.zero('BRL'),
    });
    const refundTx = WagerTransaction.create({
      ...baseProps,
      externalTransactionId: 'ext-refund',
      kind: WagerTransactionKind.Refund,
      referenceExternalTransactionId: 'ext-1',
    });
    const rollbackTx = WagerTransaction.create({
      ...baseProps,
      externalTransactionId: 'ext-rollback',
      kind: WagerTransactionKind.Rollback,
      referenceExternalTransactionId: 'ext-1',
    });

    expect(betTx.ledgerDirectionFor()).toBe(LedgerDirection.Debit);
    expect(winTx.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
    expect(refundTx.ledgerDirectionFor(betTx)).toBe(LedgerDirection.Credit);

    // ROLLBACK of BET -> CREDIT; ROLLBACK of WIN -> DEBIT; ROLLBACK of REFUND -> DEBIT
    expect(rollbackTx.ledgerDirectionFor(betTx)).toBe(LedgerDirection.Credit);
    expect(rollbackTx.ledgerDirectionFor(winTx)).toBe(LedgerDirection.Debit);
    expect(rollbackTx.ledgerDirectionFor(refundTx)).toBe(LedgerDirection.Debit);

    // LOSS does not affect balance and has no ledger direction
    expect(lossTx.affectsBalance()).toBe(false);
    expect(() => lossTx.ledgerDirectionFor()).toThrow(InvalidTransactionPropsError);
    expect(() => rollbackTx.ledgerDirectionFor(lossTx)).toThrow(
      InvalidTransactionPropsError,
    );
  });

  it('prevents state transitions once in terminal state (PROCESSED, REJECTED, FAILED)', () => {
    const tx1 = WagerTransaction.create({
      ...baseProps,
      kind: WagerTransactionKind.Bet,
    });
    tx1.markProcessed(undefined, new Date());
    expect(tx1.isTerminal()).toBe(true);

    expect(() => tx1.reject(FailureCode.InsufficientFunds)).toThrow(
      InvalidTransactionStateError,
    );
    expect(() => tx1.markPendingReference()).toThrow(InvalidTransactionStateError);
    expect(() => tx1.fail(FailureCode.InfrastructureFailure)).toThrow(
      InvalidTransactionStateError,
    );

    const tx2 = WagerTransaction.create({
      ...baseProps,
      kind: WagerTransactionKind.Bet,
    });
    tx2.reject(FailureCode.InsufficientFunds);
    expect(tx2.isTerminal()).toBe(true);
    expect(() => tx2.markProcessed(undefined, new Date())).toThrow(
      InvalidTransactionStateError,
    );
  });
});

describe('Unit — Canonical JSON Hash & Idempotency Payload Matching (Section 9)', () => {
  it('produces identical payloadHash regardless of key order and detects divergent payloads', () => {
    const hash1 = computeCanonicalPayloadHash({
      b: 2,
      a: { y: 'yes', x: 'no' },
    });
    const hash2 = computeCanonicalPayloadHash({
      a: { x: 'no', y: 'yes' },
      b: 2,
    });

    expect(toCanonicalJson({ b: 1, a: 2 })).toBe('{"a":2,"b":1}');
    expect(hash1).toBe(hash2);

    const wagerHash1 = computeWagerPayloadHash({
      providerId: 'provider-a',
      externalTransactionId: 'tx-123',
      playerId: 'player-1',
      walletId: 'wallet-1',
      roundId: 'round-1',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
    });

    const wagerHashDifferentAmount = computeWagerPayloadHash({
      providerId: 'provider-a',
      externalTransactionId: 'tx-123',
      playerId: 'player-1',
      walletId: 'wallet-1',
      roundId: 'round-1',
      gameId: 'fortune-chimp',
      kind: 'BET',
      money: { amount: '50.00', currency: 'BRL' },
    });

    expect(wagerHash1).not.toBe(wagerHashDifferentAmount);

    const tx = WagerTransaction.create({
      providerId: 'provider-a',
      externalTransactionId: 'tx-123',
      idempotencyKey: 'provider-a:tx-123',
      payloadHash: wagerHash1,
      walletId: 'wallet-1',
      playerId: 'player-1',
      roundId: 'round-1',
      gameId: 'fortune-chimp',
      kind: WagerTransactionKind.Bet,
      money: Money.from({ amount: '25.00', currency: 'BRL' }),
    });

    expect(tx.matchesPayload(wagerHash1)).toBe(true);
    expect(tx.matchesPayload(wagerHashDifferentAmount)).toBe(false);
  });
});

describe('Unit — Inbox, Outbox, IntegrationEvents & Catalog CRUD Entities (Sections 6.5 & 11)', () => {
  it('enqueues IntegrationEvent subclasses into OutboxMessage and schedules exponential retry', () => {
    const wallet = Wallet.open({
      id: 'wallet-1',
      playerId: 'player-1',
      initialBalance: Money.from({ amount: '100.00', currency: 'BRL' }),
    });
    const entry = wallet.debit({
      transactionId: 'tx-1',
      amount: Money.from({ amount: '20.00', currency: 'BRL' }),
    });

    const event = WalletBalanceChanged.from(wallet, entry, {
      correlationId: 'corr-1',
    });
    expect(event.eventType).toBe('WalletBalanceChanged');
    expect(event.version).toBe(1);
    expect(event.toJSON().data.balanceAfter).toEqual({
      amount: '80.00',
      currency: 'BRL',
    });

    const outbox = OutboxMessage.enqueue(event);
    expect(outbox.isPending()).toBe(true);
    expect(outbox.isDue(new Date(Date.now() + 1000))).toBe(true);

    const now = new Date();
    outbox.scheduleRetry(now, 200);
    expect(outbox.attempts).toBe(1);
    expect(outbox.nextAttemptAt!.getTime()).toBeGreaterThan(now.getTime());

    outbox.markPublished(now);
    expect(outbox.isPending()).toBe(false);

    const inbox = InboxMessage.receive({
      consumerName: 'consumer-a',
      messageId: 'msg-1',
      payloadHash: 'hash-1',
    });
    expect(inbox.isProcessed()).toBe(false);
    inbox.markProcessed(now);
    expect(inbox.isProcessed()).toBe(true);
  });

  it('validates GameProvider and Game domain entities for CRUD', () => {
    const provider = GameProvider.create({
      id: 'pragmatic-jungle',
      name: 'Pragmatic Jungle',
      allowedCurrencies: ['brl', 'usd'],
    });
    expect(provider.allowedCurrencies).toEqual(['BRL', 'USD']);
    provider.update({ name: 'Pragmatic Jungle Updated', status: ProviderStatus.Inactive });
    expect(provider.name).toBe('Pragmatic Jungle Updated');
    expect(provider.status).toBe(ProviderStatus.Inactive);

    const game = Game.create({
      id: 'jungle-bonanza',
      providerId: provider.id,
      name: 'Jungle Bonanza',
      minBet: Money.from({ amount: '1.00', currency: 'BRL' }),
      maxBet: Money.from({ amount: '500.00', currency: 'BRL' }),
    });
    expect(game.active).toBe(true);
    expect(() =>
      game.update({
        minBet: Money.from({ amount: '1000.00', currency: 'BRL' }),
      }),
    ).toThrow();
  });
});
