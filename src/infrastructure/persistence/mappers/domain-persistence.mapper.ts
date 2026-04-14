import Decimal from 'decimal.js';
import { Game } from '../../../domain/catalog/game';
import { GameProvider, ProviderStatus } from '../../../domain/catalog/game-provider';
import { InboxMessage } from '../../../domain/messaging/inbox-message';
import { OutboxMessage } from '../../../domain/messaging/outbox-message';
import { FailureCode } from '../../../domain/shared/domain-errors';
import { Money } from '../../../domain/shared/money';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../../domain/wagering/wager-transaction';
import { Wallet, WalletStatus } from '../../../domain/wallet/wallet';
import {
  LedgerDirection,
  WalletLedgerEntry,
} from '../../../domain/wallet/wallet-ledger-entry';
import {
  GameOrmEntity,
  GameProviderOrmEntity,
  InboxMessageOrmEntity,
  OutboxMessageOrmEntity,
  WagerTransactionOrmEntity,
  WalletLedgerEntryOrmEntity,
  WalletOrmEntity,
} from '../entities/orm-entities';

function normalizeDecimalString(raw: string | number): string {
  return new Decimal(String(raw)).toFixed(2);
}

export class DomainPersistenceMapper {
  static toDomainWallet(orm: WalletOrmEntity): Wallet {
    return Wallet.rehydrate({
      id: orm.id,
      playerId: orm.playerId,
      currency: orm.currency,
      balance: Money.rehydrate({
        amount: normalizeDecimalString(orm.balanceAmount),
        currency: orm.currency,
      }),
      version: Number(orm.version),
      status: (orm.status as WalletStatus) ?? WalletStatus.Active,
      metadata:
        typeof orm.metadata === 'string'
          ? (JSON.parse(orm.metadata) as Record<string, unknown>)
          : orm.metadata ?? {},
      createdAt: new Date(orm.createdAt),
      updatedAt: new Date(orm.updatedAt),
    });
  }

  static toOrmWallet(domain: Wallet, target = new WalletOrmEntity()): WalletOrmEntity {
    target.id = domain.id;
    target.playerId = domain.playerId;
    target.currency = domain.currency;
    target.balanceAmount = domain.balance.amount;
    target.version = domain.version;
    target.status = domain.status;
    target.metadata = { ...domain.metadata };
    target.createdAt = domain.createdAt;
    target.updatedAt = domain.updatedAt;
    return target;
  }

  static toDomainWagerTransaction(orm: WagerTransactionOrmEntity): WagerTransaction {
    return WagerTransaction.rehydrate({
      id: orm.id,
      providerId: orm.providerId,
      externalTransactionId: orm.externalTransactionId,
      idempotencyKey: orm.idempotencyKey,
      payloadHash: orm.payloadHash,
      walletId: orm.walletId,
      playerId: orm.playerId,
      roundId: orm.roundId,
      gameId: orm.gameId,
      kind: orm.kind as WagerTransactionKind,
      money: Money.rehydrate({
        amount: normalizeDecimalString(orm.moneyAmount),
        currency: orm.moneyCurrency,
      }),
      referenceExternalTransactionId: orm.referenceExternalTransactionId ?? undefined,
      createdAt: new Date(orm.createdAt),
      status: orm.status as WagerTransactionStatus,
      referenceTransactionId: orm.referenceTransactionId ?? undefined,
      failureCode: (orm.failureCode as FailureCode) ?? undefined,
      processedAt: orm.processedAt ? new Date(orm.processedAt) : undefined,
      observedBalance:
        orm.observedBalanceAmount != null && orm.observedBalanceCurrency != null
          ? Money.rehydrate({
              amount: normalizeDecimalString(orm.observedBalanceAmount),
              currency: orm.observedBalanceCurrency,
            })
          : undefined,
      retryCount: Number(orm.retryCount ?? 0),
      nextRetryAt: orm.nextRetryAt ? new Date(orm.nextRetryAt) : undefined,
    });
  }

  static toOrmWagerTransaction(
    domain: WagerTransaction,
    target = new WagerTransactionOrmEntity(),
  ): WagerTransactionOrmEntity {
    target.id = domain.id;
    target.providerId = domain.providerId;
    target.externalTransactionId = domain.externalTransactionId;
    target.idempotencyKey = domain.idempotencyKey;
    target.payloadHash = domain.payloadHash;
    target.walletId = domain.walletId;
    target.playerId = domain.playerId;
    target.roundId = domain.roundId;
    target.gameId = domain.gameId;
    target.kind = domain.kind;
    target.moneyAmount = domain.money.amount;
    target.moneyCurrency = domain.money.currency;
    target.referenceExternalTransactionId = domain.referenceExternalTransactionId ?? null;
    target.referenceTransactionId = domain.referenceTransactionId ?? null;
    target.status = domain.status;
    target.failureCode = domain.failureCode ?? null;
    target.observedBalanceAmount = domain.observedBalance?.amount ?? null;
    target.observedBalanceCurrency = domain.observedBalance?.currency ?? null;
    target.retryCount = domain.retryCount;
    target.nextRetryAt = domain.nextRetryAt ?? null;
    target.createdAt = domain.createdAt;
    target.processedAt = domain.processedAt ?? null;
    return target;
  }

  static toDomainLedgerEntry(orm: WalletLedgerEntryOrmEntity): WalletLedgerEntry {
    return WalletLedgerEntry.rehydrate({
      id: orm.id,
      walletId: orm.walletId,
      transactionId: orm.transactionId,
      direction: orm.direction as LedgerDirection,
      money: Money.rehydrate({
        amount: normalizeDecimalString(orm.moneyAmount),
        currency: orm.moneyCurrency,
      }),
      balanceBefore: Money.rehydrate({
        amount: normalizeDecimalString(orm.balanceBeforeAmount),
        currency: orm.moneyCurrency,
      }),
      balanceAfter: Money.rehydrate({
        amount: normalizeDecimalString(orm.balanceAfterAmount),
        currency: orm.moneyCurrency,
      }),
      createdAt: new Date(orm.createdAt),
    });
  }

  static toOrmLedgerEntry(
    domain: WalletLedgerEntry,
    target = new WalletLedgerEntryOrmEntity(),
  ): WalletLedgerEntryOrmEntity {
    target.id = domain.id;
    target.walletId = domain.walletId;
    target.transactionId = domain.transactionId;
    target.direction = domain.direction;
    target.moneyAmount = domain.money.amount;
    target.moneyCurrency = domain.money.currency;
    target.balanceBeforeAmount = domain.balanceBefore.amount;
    target.balanceAfterAmount = domain.balanceAfter.amount;
    target.createdAt = domain.createdAt;
    return target;
  }

  static toDomainInboxMessage(orm: InboxMessageOrmEntity): InboxMessage {
    return InboxMessage.rehydrate({
      consumerName: orm.consumerName,
      messageId: orm.messageId,
      payloadHash: orm.payloadHash,
      receivedAt: new Date(orm.receivedAt),
      processedAt: orm.processedAt ? new Date(orm.processedAt) : undefined,
    });
  }

  static toOrmInboxMessage(
    domain: InboxMessage,
    target = new InboxMessageOrmEntity(),
  ): InboxMessageOrmEntity {
    target.consumerName = domain.consumerName;
    target.messageId = domain.messageId;
    target.payloadHash = domain.payloadHash;
    target.receivedAt = domain.receivedAt;
    target.processedAt = domain.processedAt ?? null;
    return target;
  }

  static toDomainOutboxMessage(orm: OutboxMessageOrmEntity): OutboxMessage {
    const parsedPayload =
      typeof orm.payload === 'string'
        ? (JSON.parse(orm.payload) as Record<string, unknown>)
        : orm.payload;
    return OutboxMessage.rehydrate({
      id: orm.id,
      aggregateId: orm.aggregateId,
      eventType: orm.eventType,
      payload: parsedPayload,
      occurredAt: new Date(orm.occurredAt),
      attempts: Number(orm.attempts ?? 0),
      nextAttemptAt: orm.nextAttemptAt ? new Date(orm.nextAttemptAt) : undefined,
      publishedAt: orm.publishedAt ? new Date(orm.publishedAt) : undefined,
    });
  }

  static toOrmOutboxMessage(
    domain: OutboxMessage,
    target = new OutboxMessageOrmEntity(),
  ): OutboxMessageOrmEntity {
    target.id = domain.id;
    target.aggregateId = domain.aggregateId;
    target.eventType = domain.eventType;
    target.payload = { ...domain.payload };
    target.occurredAt = domain.occurredAt;
    target.attempts = domain.attempts;
    target.nextAttemptAt = domain.nextAttemptAt ?? null;
    target.publishedAt = domain.publishedAt ?? null;
    return target;
  }

  static toDomainGameProvider(orm: GameProviderOrmEntity): GameProvider {
    const allowed = Array.isArray(orm.allowedCurrencies)
      ? orm.allowedCurrencies
      : typeof orm.allowedCurrencies === 'string'
        ? (JSON.parse(orm.allowedCurrencies) as string[])
        : ['BRL'];
    return GameProvider.rehydrate({
      id: orm.id,
      name: orm.name,
      status: orm.status as ProviderStatus,
      webhookUrl: orm.webhookUrl ?? undefined,
      allowedCurrencies: allowed,
      createdAt: new Date(orm.createdAt),
      updatedAt: new Date(orm.updatedAt),
    });
  }

  static toOrmGameProvider(
    domain: GameProvider,
    target = new GameProviderOrmEntity(),
  ): GameProviderOrmEntity {
    target.id = domain.id;
    target.name = domain.name;
    target.status = domain.status;
    target.webhookUrl = domain.webhookUrl ?? null;
    target.allowedCurrencies = [...domain.allowedCurrencies];
    target.createdAt = domain.createdAt;
    target.updatedAt = domain.updatedAt;
    return target;
  }

  static toDomainGame(orm: GameOrmEntity): Game {
    return Game.rehydrate({
      id: orm.id,
      providerId: orm.providerId,
      name: orm.name,
      category: orm.category,
      active: Boolean(orm.active),
      minBet: Money.rehydrate({
        amount: normalizeDecimalString(orm.minBetAmount),
        currency: orm.currency,
      }),
      maxBet: Money.rehydrate({
        amount: normalizeDecimalString(orm.maxBetAmount),
        currency: orm.currency,
      }),
      createdAt: new Date(orm.createdAt),
      updatedAt: new Date(orm.updatedAt),
    });
  }

  static toOrmGame(domain: Game, target = new GameOrmEntity()): GameOrmEntity {
    target.id = domain.id;
    target.providerId = domain.providerId;
    target.name = domain.name;
    target.category = domain.category;
    target.active = domain.active;
    target.minBetAmount = domain.minBet.amount;
    target.maxBetAmount = domain.maxBet.amount;
    target.currency = domain.minBet.currency;
    target.createdAt = domain.createdAt;
    target.updatedAt = domain.updatedAt;
    return target;
  }
}
