import { EntitySchema } from '@mikro-orm/core';

export class WalletOrmEntity {
  id!: string;
  playerId!: string;
  currency!: string;
  balanceAmount!: string;
  version!: number;
  status!: string;
  metadata!: Record<string, unknown>;
  createdAt!: Date;
  updatedAt!: Date;
}

export const WalletEntitySchema = new EntitySchema<WalletOrmEntity>({
  class: WalletOrmEntity,
  tableName: 'wallets',
  properties: {
    id: { type: 'uuid', primary: true },
    playerId: { type: 'string', fieldName: 'player_id', length: 128 },
    currency: { type: 'string', length: 3 },
    balanceAmount: {
      type: 'decimal',
      fieldName: 'balance_amount',
      precision: 20,
      scale: 2,
    },
    version: { type: 'integer', default: 1 },
    status: { type: 'string', length: 32, default: 'ACTIVE' },
    metadata: { type: 'json', default: '{}' },
    createdAt: { type: 'Date', fieldName: 'created_at' },
    updatedAt: { type: 'Date', fieldName: 'updated_at' },
  },
});

export class WagerTransactionOrmEntity {
  id!: string;
  providerId!: string;
  externalTransactionId!: string;
  idempotencyKey!: string;
  payloadHash!: string;
  walletId!: string;
  playerId!: string;
  roundId!: string;
  gameId!: string;
  kind!: string;
  moneyAmount!: string;
  moneyCurrency!: string;
  referenceExternalTransactionId?: string | null;
  referenceTransactionId?: string | null;
  status!: string;
  failureCode?: string | null;
  observedBalanceAmount?: string | null;
  observedBalanceCurrency?: string | null;
  retryCount!: number;
  nextRetryAt?: Date | null;
  createdAt!: Date;
  processedAt?: Date | null;
}

export const WagerTransactionEntitySchema = new EntitySchema<WagerTransactionOrmEntity>({
  class: WagerTransactionOrmEntity,
  tableName: 'wager_transactions',
  properties: {
    id: { type: 'uuid', primary: true },
    providerId: { type: 'string', fieldName: 'provider_id', length: 128 },
    externalTransactionId: {
      type: 'string',
      fieldName: 'external_transaction_id',
      length: 128,
    },
    idempotencyKey: {
      type: 'string',
      fieldName: 'idempotency_key',
      length: 255,
    },
    payloadHash: { type: 'string', fieldName: 'payload_hash', length: 64 },
    walletId: { type: 'uuid', fieldName: 'wallet_id' },
    playerId: { type: 'string', fieldName: 'player_id', length: 128 },
    roundId: { type: 'string', fieldName: 'round_id', length: 128 },
    gameId: { type: 'string', fieldName: 'game_id', length: 128 },
    kind: { type: 'string', length: 32 },
    moneyAmount: {
      type: 'decimal',
      fieldName: 'money_amount',
      precision: 20,
      scale: 2,
    },
    moneyCurrency: { type: 'string', fieldName: 'money_currency', length: 3 },
    referenceExternalTransactionId: {
      type: 'string',
      fieldName: 'reference_external_transaction_id',
      length: 128,
      nullable: true,
    },
    referenceTransactionId: {
      type: 'uuid',
      fieldName: 'reference_transaction_id',
      nullable: true,
    },
    status: { type: 'string', length: 32 },
    failureCode: {
      type: 'string',
      fieldName: 'failure_code',
      length: 64,
      nullable: true,
    },
    observedBalanceAmount: {
      type: 'decimal',
      fieldName: 'observed_balance_amount',
      precision: 20,
      scale: 2,
      nullable: true,
    },
    observedBalanceCurrency: {
      type: 'string',
      fieldName: 'observed_balance_currency',
      length: 3,
      nullable: true,
    },
    retryCount: { type: 'integer', fieldName: 'retry_count', default: 0 },
    nextRetryAt: { type: 'Date', fieldName: 'next_retry_at', nullable: true },
    createdAt: { type: 'Date', fieldName: 'created_at' },
    processedAt: { type: 'Date', fieldName: 'processed_at', nullable: true },
  },
});

export class WalletLedgerEntryOrmEntity {
  id!: string;
  walletId!: string;
  transactionId!: string;
  direction!: string;
  moneyAmount!: string;
  moneyCurrency!: string;
  balanceBeforeAmount!: string;
  balanceAfterAmount!: string;
  createdAt!: Date;
}

export const WalletLedgerEntryEntitySchema = new EntitySchema<WalletLedgerEntryOrmEntity>({
  class: WalletLedgerEntryOrmEntity,
  tableName: 'wallet_ledger_entries',
  properties: {
    id: { type: 'uuid', primary: true },
    walletId: { type: 'uuid', fieldName: 'wallet_id' },
    transactionId: { type: 'uuid', fieldName: 'transaction_id' },
    direction: { type: 'string', length: 16 },
    moneyAmount: {
      type: 'decimal',
      fieldName: 'money_amount',
      precision: 20,
      scale: 2,
    },
    moneyCurrency: { type: 'string', fieldName: 'money_currency', length: 3 },
    balanceBeforeAmount: {
      type: 'decimal',
      fieldName: 'balance_before_amount',
      precision: 20,
      scale: 2,
    },
    balanceAfterAmount: {
      type: 'decimal',
      fieldName: 'balance_after_amount',
      precision: 20,
      scale: 2,
    },
    createdAt: { type: 'Date', fieldName: 'created_at' },
  },
});

export class InboxMessageOrmEntity {
  consumerName!: string;
  messageId!: string;
  payloadHash!: string;
  receivedAt!: Date;
  processedAt?: Date | null;
}

export const InboxMessageEntitySchema = new EntitySchema<InboxMessageOrmEntity>({
  class: InboxMessageOrmEntity,
  tableName: 'inbox_messages',
  properties: {
    consumerName: {
      type: 'string',
      fieldName: 'consumer_name',
      length: 128,
      primary: true,
    },
    messageId: {
      type: 'string',
      fieldName: 'message_id',
      length: 128,
      primary: true,
    },
    payloadHash: { type: 'string', fieldName: 'payload_hash', length: 64 },
    receivedAt: { type: 'Date', fieldName: 'received_at' },
    processedAt: { type: 'Date', fieldName: 'processed_at', nullable: true },
  },
});

export class OutboxMessageOrmEntity {
  id!: string;
  aggregateId!: string;
  eventType!: string;
  payload!: Record<string, unknown>;
  occurredAt!: Date;
  attempts!: number;
  nextAttemptAt?: Date | null;
  publishedAt?: Date | null;
}

export const OutboxMessageEntitySchema = new EntitySchema<OutboxMessageOrmEntity>({
  class: OutboxMessageOrmEntity,
  tableName: 'outbox_messages',
  properties: {
    id: { type: 'uuid', primary: true },
    aggregateId: { type: 'string', fieldName: 'aggregate_id', length: 128 },
    eventType: { type: 'string', fieldName: 'event_type', length: 128 },
    payload: { type: 'json' },
    occurredAt: { type: 'Date', fieldName: 'occurred_at' },
    attempts: { type: 'integer', default: 0 },
    nextAttemptAt: {
      type: 'Date',
      fieldName: 'next_attempt_at',
      nullable: true,
    },
    publishedAt: { type: 'Date', fieldName: 'published_at', nullable: true },
  },
});

export class GameProviderOrmEntity {
  id!: string;
  name!: string;
  status!: string;
  webhookUrl?: string | null;
  allowedCurrencies!: string[];
  createdAt!: Date;
  updatedAt!: Date;
}

export const GameProviderEntitySchema = new EntitySchema<GameProviderOrmEntity>({
  class: GameProviderOrmEntity,
  tableName: 'game_providers',
  properties: {
    id: { type: 'string', length: 128, primary: true },
    name: { type: 'string', length: 255 },
    status: { type: 'string', length: 32, default: 'ACTIVE' },
    webhookUrl: {
      type: 'string',
      fieldName: 'webhook_url',
      length: 512,
      nullable: true,
    },
    allowedCurrencies: {
      type: 'json',
      fieldName: 'allowed_currencies',
    },
    createdAt: { type: 'Date', fieldName: 'created_at' },
    updatedAt: { type: 'Date', fieldName: 'updated_at' },
  },
});

export class GameOrmEntity {
  id!: string;
  providerId!: string;
  name!: string;
  category!: string;
  active!: boolean;
  minBetAmount!: string;
  maxBetAmount!: string;
  currency!: string;
  createdAt!: Date;
  updatedAt!: Date;
}

export const GameEntitySchema = new EntitySchema<GameOrmEntity>({
  class: GameOrmEntity,
  tableName: 'games',
  properties: {
    id: { type: 'string', length: 128, primary: true },
    providerId: { type: 'string', fieldName: 'provider_id', length: 128 },
    name: { type: 'string', length: 255 },
    category: { type: 'string', length: 64 },
    active: { type: 'boolean', default: true },
    minBetAmount: {
      type: 'decimal',
      fieldName: 'min_bet_amount',
      precision: 20,
      scale: 2,
    },
    maxBetAmount: {
      type: 'decimal',
      fieldName: 'max_bet_amount',
      precision: 20,
      scale: 2,
    },
    currency: { type: 'string', length: 3 },
    createdAt: { type: 'Date', fieldName: 'created_at' },
    updatedAt: { type: 'Date', fieldName: 'updated_at' },
  },
});

export const ALL_ORM_SCHEMAS = [
  WalletEntitySchema,
  WagerTransactionEntitySchema,
  WalletLedgerEntryEntitySchema,
  InboxMessageEntitySchema,
  OutboxMessageEntitySchema,
  GameProviderEntitySchema,
  GameEntitySchema,
];
