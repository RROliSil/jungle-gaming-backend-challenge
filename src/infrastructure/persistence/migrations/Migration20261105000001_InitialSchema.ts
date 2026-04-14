import { Migration } from '@mikro-orm/migrations';

export const INITIAL_SCHEMA_UP_SQL = `
CREATE TABLE IF NOT EXISTS game_providers (
  id VARCHAR(128) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
  webhook_url VARCHAR(512),
  allowed_currencies JSONB NOT NULL DEFAULT '["BRL"]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_game_providers_status CHECK (status IN ('ACTIVE', 'INACTIVE'))
);

CREATE TABLE IF NOT EXISTS games (
  id VARCHAR(128) PRIMARY KEY,
  provider_id VARCHAR(128) NOT NULL REFERENCES game_providers(id) ON DELETE RESTRICT,
  name VARCHAR(255) NOT NULL,
  category VARCHAR(64) NOT NULL DEFAULT 'SLOTS',
  active BOOLEAN NOT NULL DEFAULT TRUE,
  min_bet_amount NUMERIC(20, 2) NOT NULL DEFAULT 0.10,
  max_bet_amount NUMERIC(20, 2) NOT NULL DEFAULT 10000.00,
  currency VARCHAR(3) NOT NULL DEFAULT 'BRL',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_games_bet_limits CHECK (min_bet_amount >= 0 AND max_bet_amount >= min_bet_amount)
);

CREATE TABLE IF NOT EXISTS wallets (
  id UUID PRIMARY KEY,
  player_id VARCHAR(128) NOT NULL,
  currency VARCHAR(3) NOT NULL,
  balance_amount NUMERIC(20, 2) NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_wallets_player_currency UNIQUE (player_id, currency),
  CONSTRAINT chk_wallets_balance_non_negative CHECK (balance_amount >= 0),
  CONSTRAINT chk_wallets_version_positive CHECK (version >= 1),
  CONSTRAINT chk_wallets_currency_iso CHECK (char_length(currency) = 3),
  CONSTRAINT chk_wallets_status CHECK (status IN ('ACTIVE', 'FROZEN', 'CLOSED'))
);

CREATE INDEX IF NOT EXISTS idx_wallets_player_id ON wallets (player_id);

CREATE TABLE IF NOT EXISTS wager_transactions (
  id UUID PRIMARY KEY,
  provider_id VARCHAR(128) NOT NULL,
  external_transaction_id VARCHAR(128) NOT NULL,
  idempotency_key VARCHAR(255) NOT NULL,
  payload_hash VARCHAR(64) NOT NULL,
  wallet_id UUID NOT NULL REFERENCES wallets(id) ON DELETE RESTRICT,
  player_id VARCHAR(128) NOT NULL,
  round_id VARCHAR(128) NOT NULL,
  game_id VARCHAR(128) NOT NULL,
  kind VARCHAR(32) NOT NULL,
  money_amount NUMERIC(20, 2) NOT NULL,
  money_currency VARCHAR(3) NOT NULL,
  reference_external_transaction_id VARCHAR(128),
  reference_transaction_id UUID REFERENCES wager_transactions(id) ON DELETE RESTRICT,
  status VARCHAR(32) NOT NULL,
  failure_code VARCHAR(64),
  observed_balance_amount NUMERIC(20, 2),
  observed_balance_currency VARCHAR(3),
  retry_count INTEGER NOT NULL DEFAULT 0,
  next_retry_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMPTZ,
  CONSTRAINT uq_wager_tx_idempotency UNIQUE (idempotency_key),
  CONSTRAINT uq_wager_tx_provider_ext UNIQUE (provider_id, external_transaction_id),
  CONSTRAINT chk_wager_tx_kind CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
  CONSTRAINT chk_wager_tx_status CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
  CONSTRAINT chk_wager_tx_amount_non_negative CHECK (money_amount >= 0),
  CONSTRAINT chk_wager_tx_positive_unless_loss CHECK ((kind <> 'LOSS' AND money_amount > 0) OR (kind = 'LOSS' AND money_amount >= 0)),
  CONSTRAINT chk_wager_tx_reference_required CHECK ((kind IN ('REFUND', 'ROLLBACK') AND reference_external_transaction_id IS NOT NULL) OR (kind NOT IN ('REFUND', 'ROLLBACK')))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_single_processed_reversal
  ON wager_transactions (reference_transaction_id, kind)
  WHERE status = 'PROCESSED' AND reference_transaction_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_wager_tx_wallet_id ON wager_transactions (wallet_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_wager_tx_pending_ref ON wager_transactions (status, next_retry_at) WHERE status = 'PENDING_REFERENCE';
CREATE INDEX IF NOT EXISTS idx_wager_tx_round_id ON wager_transactions (round_id);

CREATE TABLE IF NOT EXISTS wallet_ledger_entries (
  id UUID PRIMARY KEY,
  wallet_id UUID NOT NULL REFERENCES wallets(id) ON DELETE RESTRICT,
  transaction_id UUID NOT NULL REFERENCES wager_transactions(id) ON DELETE RESTRICT,
  direction VARCHAR(16) NOT NULL,
  money_amount NUMERIC(20, 2) NOT NULL,
  money_currency VARCHAR(3) NOT NULL,
  balance_before_amount NUMERIC(20, 2) NOT NULL,
  balance_after_amount NUMERIC(20, 2) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_ledger_wallet_tx UNIQUE (wallet_id, transaction_id),
  CONSTRAINT chk_ledger_direction CHECK (direction IN ('DEBIT', 'CREDIT')),
  CONSTRAINT chk_ledger_money_positive CHECK (money_amount > 0),
  CONSTRAINT chk_ledger_balances_non_negative CHECK (balance_before_amount >= 0 AND balance_after_amount >= 0),
  CONSTRAINT chk_ledger_arithmetic CHECK (
    (direction = 'DEBIT' AND balance_before_amount - money_amount = balance_after_amount)
    OR (direction = 'CREDIT' AND balance_before_amount + money_amount = balance_after_amount)
  )
);

CREATE INDEX IF NOT EXISTS idx_ledger_wallet_cursor ON wallet_ledger_entries (wallet_id, created_at ASC, id ASC);

CREATE TABLE IF NOT EXISTS inbox_messages (
  consumer_name VARCHAR(128) NOT NULL,
  message_id VARCHAR(128) NOT NULL,
  payload_hash VARCHAR(64) NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  processed_at TIMESTAMPTZ,
  PRIMARY KEY (consumer_name, message_id)
);

CREATE TABLE IF NOT EXISTS outbox_messages (
  id UUID PRIMARY KEY,
  aggregate_id VARCHAR(128) NOT NULL,
  event_type VARCHAR(128) NOT NULL,
  payload JSONB NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ,
  published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending
  ON outbox_messages (next_attempt_at ASC, occurred_at ASC)
  WHERE published_at IS NULL;

-- Immutability Trigger for wallet_ledger_entries (Section 5 Rule 5 & 9)
CREATE OR REPLACE FUNCTION fn_prevent_ledger_mutation()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'wallet_ledger_entries is immutable: % operation is forbidden on ledger entry %', TG_OP, OLD.id
    USING ERRCODE = '55000';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_immutable_ledger ON wallet_ledger_entries;
CREATE TRIGGER trg_immutable_ledger
BEFORE UPDATE OR DELETE ON wallet_ledger_entries
FOR EACH ROW EXECUTE FUNCTION fn_prevent_ledger_mutation();

-- Terminal State Protection Trigger for wager_transactions (Section 6.3)
CREATE OR REPLACE FUNCTION fn_prevent_terminal_tx_mutation()
RETURNS TRIGGER AS $$
BEGIN
  IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') AND NEW.status <> OLD.status THEN
    RAISE EXCEPTION 'wager_transactions in terminal status (%) cannot transition to % (tx: %)', OLD.status, NEW.status, OLD.id
      USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_terminal_tx_mutation ON wager_transactions;
CREATE TRIGGER trg_prevent_terminal_tx_mutation
BEFORE UPDATE ON wager_transactions
FOR EACH ROW EXECUTE FUNCTION fn_prevent_terminal_tx_mutation();

-- Seed default provider and game so challenge payloads work out-of-the-box
INSERT INTO game_providers (id, name, status, allowed_currencies)
VALUES ('provider-a', 'Jungle Provider A', 'ACTIVE', '["BRL", "USD", "EUR"]'::jsonb)
ON CONFLICT (id) DO NOTHING;

INSERT INTO games (id, provider_id, name, category, active, min_bet_amount, max_bet_amount, currency)
VALUES ('fortune-chimp', 'provider-a', 'Fortune Chimp', 'SLOTS', TRUE, 0.10, 50000.00, 'BRL')
ON CONFLICT (id) DO NOTHING;
`;

export const INITIAL_SCHEMA_DOWN_SQL = `
DROP TRIGGER IF EXISTS trg_prevent_terminal_tx_mutation ON wager_transactions;
DROP FUNCTION IF EXISTS fn_prevent_terminal_tx_mutation();
DROP TRIGGER IF EXISTS trg_immutable_ledger ON wallet_ledger_entries;
DROP FUNCTION IF EXISTS fn_prevent_ledger_mutation();
DROP TABLE IF EXISTS outbox_messages;
DROP TABLE IF EXISTS inbox_messages;
DROP TABLE IF EXISTS wallet_ledger_entries;
DROP TABLE IF EXISTS wager_transactions;
DROP TABLE IF EXISTS wallets;
DROP TABLE IF EXISTS games;
DROP TABLE IF EXISTS game_providers;
`;

export class Migration20261105000001_InitialSchema extends Migration {
  override async up(): Promise<void> {
    this.addSql(INITIAL_SCHEMA_UP_SQL);
  }

  override async down(): Promise<void> {
    this.addSql(INITIAL_SCHEMA_DOWN_SQL);
  }
}
