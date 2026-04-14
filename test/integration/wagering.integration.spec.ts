import { randomUUID } from 'crypto';
import { EntityManager } from '@mikro-orm/core';
import { INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { afterAll, beforeAll, describe, expect, it } from 'bun:test';
import { AppModule } from '../../src/app.module';
import { FailureCode } from '../../src/domain/shared/domain-errors';
import {
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../src/domain/wagering/wager-transaction';
import { OutboxWorker } from '../../src/infrastructure/messaging/outbox.worker';
import { PendingReferenceWorker } from '../../src/infrastructure/messaging/pending-reference.worker';
import { SqsClientService } from '../../src/infrastructure/messaging/sqs-client.service';
import { SqsWagerConsumer } from '../../src/infrastructure/messaging/sqs-wager.consumer';
import { MetricsService } from '../../src/infrastructure/observability/metrics.service';
import {
  INITIAL_SCHEMA_DOWN_SQL,
  INITIAL_SCHEMA_UP_SQL,
} from '../../src/infrastructure/persistence/migrations/Migration20261105000001_InitialSchema';
import { WageringWebsocketGateway } from '../../src/infrastructure/websocket/wagering.gateway';
import {
  ensureTestInfrastructure,
  shutdownTestInfrastructure,
  startNestTestServer,
} from '../helpers/test-infra';

describe('Integration — Migrations, Constraints, HTTP API, SQS Inbox/Outbox, CRUD & WebSockets (Section 13)', () => {
  let app: INestApplication;
  let baseUrl: string;
  let em: EntityManager;
  let sqsClient: SqsClientService;
  let sqsConsumer: SqsWagerConsumer;
  let outboxWorker: OutboxWorker;
  let pendingRefWorker: PendingReferenceWorker;
  let wsGateway: WageringWebsocketGateway;
  let metricsService: MetricsService;
  let resetQueues: () => void;

  beforeAll(async () => {
    const infra = await ensureTestInfrastructure();
    resetQueues = infra.resetSqsQueues;

    app = await NestFactory.create(AppModule.register(), {
      logger: false,
    });

    em = app.get(EntityManager);
    sqsClient = app.get(SqsClientService);
    sqsConsumer = app.get(SqsWagerConsumer);
    outboxWorker = app.get(OutboxWorker);
    pendingRefWorker = app.get(PendingReferenceWorker);
    wsGateway = app.get(WageringWebsocketGateway);
    metricsService = app.get(MetricsService);

    sqsClient.refreshClient();

    // Test reversible migrations: down -> up
    await em.getConnection().execute(INITIAL_SCHEMA_DOWN_SQL);
    await em.getConnection().execute(INITIAL_SCHEMA_UP_SQL);
    await sqsClient.ensureQueuesExist();
    baseUrl = await startNestTestServer(app);
  });

  afterAll(async () => {
    if (app) {
      await app.close();
    }
    await shutdownTestInfrastructure();
  });

  it('enforces database schema constraints and triggers: non-negative balance, unique wallet, and immutable ledger', async () => {
    const conn = em.getConnection();
    const playerId = `player-schema-${randomUUID()}`;
    const walletId1 = randomUUID();
    const walletId2 = randomUUID();

    // 1. Create wallet via SQL
    await conn.execute(
      `INSERT INTO wallets (id, player_id, currency, balance_amount, version)
       VALUES (?, ?, 'BRL', 100.00, 1)`,
      [walletId1, playerId],
    );

    // 2. Duplicate (player_id, currency) violates uq_wallets_player_currency
    let duplicateFailed = false;
    try {
      await conn.execute(
        `INSERT INTO wallets (id, player_id, currency, balance_amount, version)
         VALUES (?, ?, 'BRL', 50.00, 1)`,
        [walletId2, playerId],
      );
    } catch {
      duplicateFailed = true;
    }
    expect(duplicateFailed).toBe(true);

    // 3. Negative balance violates chk_wallets_balance_non_negative
    let negativeFailed = false;
    try {
      await conn.execute(
        `UPDATE wallets SET balance_amount = -1.00 WHERE id = ?`,
        [walletId1],
      );
    } catch {
      negativeFailed = true;
    }
    expect(negativeFailed).toBe(true);

    // 4. Insert valid transaction and ledger entry, then verify UPDATE and DELETE are blocked by trigger trg_immutable_ledger
    const txId = randomUUID();
    const entryId = randomUUID();
    await conn.execute(
      `INSERT INTO wager_transactions (
        id, provider_id, external_transaction_id, idempotency_key, payload_hash,
        wallet_id, player_id, round_id, game_id, kind, money_amount, money_currency, status
      ) VALUES (?, 'provider-a', ?, ?, 'hash1', ?, ?, 'r1', 'fortune-chimp', 'BET', 20.00, 'BRL', 'PROCESSED')`,
      [txId, `ext-${txId}`, `idem-${txId}`, walletId1, playerId],
    );

    await conn.execute(
      `INSERT INTO wallet_ledger_entries (
        id, wallet_id, transaction_id, direction, money_amount, money_currency, balance_before_amount, balance_after_amount
      ) VALUES (?, ?, ?, 'DEBIT', 20.00, 'BRL', 100.00, 80.00)`,
      [entryId, walletId1, txId],
    );

    let ledgerUpdateBlocked = false;
    try {
      await conn.execute(
        `UPDATE wallet_ledger_entries SET money_amount = 10.00 WHERE id = ?`,
        [entryId],
      );
    } catch {
      ledgerUpdateBlocked = true;
    }
    expect(ledgerUpdateBlocked).toBe(true);

    let ledgerDeleteBlocked = false;
    try {
      await conn.execute(`DELETE FROM wallet_ledger_entries WHERE id = ?`, [entryId]);
    } catch {
      ledgerDeleteBlocked = true;
    }
    expect(ledgerDeleteBlocked).toBe(true);

    // 5. Verify terminal transaction state mutation is blocked by trigger trg_prevent_terminal_tx_mutation
    let terminalMutationBlocked = false;
    try {
      await conn.execute(
        `UPDATE wager_transactions SET status = 'PENDING' WHERE id = ?`,
        [txId],
      );
    } catch {
      terminalMutationBlocked = true;
    }
    expect(terminalMutationBlocked).toBe(true);
  });

  it('executes full HTTP lifecycle: Wallet creation (OPENING), BET, WIN, LOSS, REFUND, ROLLBACK, Idempotency, Ledger Cursor & Reconciliation', async () => {
    const playerId = randomUUID();

    // 1. POST /wallets with 1000.00 BRL
    const createWalletRes = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: '1000.00', currency: 'BRL' },
      }),
    });
    expect(createWalletRes.status).toBe(201);
    const wallet = (await createWalletRes.json()) as {
      id: string;
      playerId: string;
      balance: { amount: string; currency: string };
      version: number;
    };
    expect(wallet.balance).toEqual({ amount: '1000.00', currency: 'BRL' });
    expect(wallet.version).toBe(1);

    // Duplicate wallet for same playerId + currency returns 409 Conflict
    const dupWalletRes = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: '500.00', currency: 'BRL' },
      }),
    });
    expect(dupWalletRes.status).toBe(409);

    // 2. Submit BET of 100.00 BRL
    const betExtId = `bet-${randomUUID()}`;
    const betIdemKey = `provider-a:${betExtId}`;
    const betPayload = {
      providerId: 'provider-a',
      externalTransactionId: betExtId,
      playerId,
      walletId: wallet.id,
      roundId: 'round-100',
      gameId: 'fortune-chimp',
      kind: WagerTransactionKind.Bet,
      money: { amount: '100.00', currency: 'BRL' },
    };

    const betRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': betIdemKey,
      },
      body: JSON.stringify(betPayload),
    });
    expect(betRes.status).toBe(200);
    const betData = (await betRes.json()) as {
      transactionId: string;
      status: string;
      balance: { amount: string; currency: string };
      idempotentReplay: boolean;
    };
    expect(betData.status).toBe(WagerTransactionStatus.Processed);
    expect(betData.balance).toEqual({ amount: '900.00', currency: 'BRL' });
    expect(betData.idempotentReplay).toBe(false);

    // 3. Submit WIN of 250.00 BRL referencing the BET
    const winExtId = `win-${randomUUID()}`;
    const winRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${winExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: winExtId,
        playerId,
        walletId: wallet.id,
        roundId: 'round-100',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Win,
        money: { amount: '250.00', currency: 'BRL' },
        referenceExternalTransactionId: betExtId,
      }),
    });
    expect(winRes.status).toBe(200);
    const winData = (await winRes.json()) as {
      transactionId: string;
      status: string;
      balance: { amount: string; currency: string };
    };
    expect(winData.status).toBe(WagerTransactionStatus.Processed);
    expect(winData.balance).toEqual({ amount: '1150.00', currency: 'BRL' });

    // 4. Replay the original BET -> Rule 7.7: returns original result INCLUDING observed balance at that time (900.00 BRL) and idempotentReplay: true
    const replayRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': betIdemKey,
      },
      body: JSON.stringify(betPayload),
    });
    expect(replayRes.status).toBe(200);
    const replayData = (await replayRes.json()) as {
      transactionId: string;
      status: string;
      balance: { amount: string; currency: string };
      idempotentReplay: boolean;
    };
    expect(replayData.idempotentReplay).toBe(true);
    expect(replayData.transactionId).toBe(betData.transactionId);
    expect(replayData.balance).toEqual({ amount: '900.00', currency: 'BRL' });

    // 5. Same Idempotency-Key with divergent payload -> 409 Conflict
    const conflictRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': betIdemKey,
      },
      body: JSON.stringify({
        ...betPayload,
        money: { amount: '999.00', currency: 'BRL' },
      }),
    });
    expect(conflictRes.status).toBe(409);

    // 6. Submit LOSS (0.00 BRL) -> affectsBalance = false, no ledger entry
    const lossExtId = `loss-${randomUUID()}`;
    const lossRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${lossExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: lossExtId,
        playerId,
        walletId: wallet.id,
        roundId: 'round-100',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Loss,
        money: { amount: '0.00', currency: 'BRL' },
        referenceExternalTransactionId: betExtId,
      }),
    });
    expect(lossRes.status).toBe(200);
    const lossData = (await lossRes.json()) as {
      status: string;
      balance: { amount: string; currency: string };
    };
    expect(lossData.status).toBe(WagerTransactionStatus.Processed);
    expect(lossData.balance).toEqual({ amount: '1150.00', currency: 'BRL' });

    // 7. Submit REFUND of the BET (100.00 BRL) -> balance becomes 1250.00 BRL
    const refundExtId = `refund-${randomUUID()}`;
    const refundRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${refundExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: refundExtId,
        playerId,
        walletId: wallet.id,
        roundId: 'round-100',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Refund,
        money: { amount: '100.00', currency: 'BRL' },
        referenceExternalTransactionId: betExtId,
      }),
    });
    expect(refundRes.status).toBe(200);
    const refundData = (await refundRes.json()) as {
      status: string;
      balance: { amount: string; currency: string };
    };
    expect(refundData.status).toBe(WagerTransactionStatus.Processed);
    expect(refundData.balance).toEqual({ amount: '1250.00', currency: 'BRL' });

    // 8. Second REFUND on the same BET must be REJECTED (422) with REFERENCE_ALREADY_REVERSED
    const dupRefundExtId = `refund-dup-${randomUUID()}`;
    const dupRefundRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${dupRefundExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: dupRefundExtId,
        playerId,
        walletId: wallet.id,
        roundId: 'round-100',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Refund,
        money: { amount: '100.00', currency: 'BRL' },
        referenceExternalTransactionId: betExtId,
      }),
    });
    expect(dupRefundRes.status).toBe(422);
    const dupRefundData = (await dupRefundRes.json()) as {
      status: string;
      failureCode: string;
    };
    expect(dupRefundData.status).toBe(WagerTransactionStatus.Rejected);
    expect(dupRefundData.failureCode).toBe(FailureCode.ReferenceAlreadyReversed);

    // 9. ROLLBACK of the WIN (250.00 BRL) -> debits 250.00 BRL -> balance becomes 1000.00 BRL
    const rollbackWinExtId = `rollback-win-${randomUUID()}`;
    const rollbackWinRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${rollbackWinExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: rollbackWinExtId,
        playerId,
        walletId: wallet.id,
        roundId: 'round-100',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Rollback,
        money: { amount: '250.00', currency: 'BRL' },
        referenceExternalTransactionId: winExtId,
      }),
    });
    expect(rollbackWinRes.status).toBe(200);
    const rollbackWinData = (await rollbackWinRes.json()) as {
      status: string;
      balance: { amount: string; currency: string };
    };
    expect(rollbackWinData.status).toBe(WagerTransactionStatus.Processed);
    expect(rollbackWinData.balance).toEqual({ amount: '1000.00', currency: 'BRL' });

    // 10. Query transaction endpoints
    const getTxByIdRes = await fetch(
      `${baseUrl}/wagering/transactions/${betData.transactionId}`,
    );
    expect(getTxByIdRes.status).toBe(200);

    const getTxByExtRes = await fetch(
      `${baseUrl}/providers/provider-a/wagering/transactions/${betExtId}`,
    );
    expect(getTxByExtRes.status).toBe(200);

    // 11. Query Ledger with cursor pagination (limit=2)
    // Expected ledger entries: 1 OPENING (+1000), 1 BET (-100), 1 WIN (+250), 1 REFUND (+100), 1 ROLLBACK (-250) = 5 entries (LOSS and REJECTED generated 0 entries!)
    const ledgerPage1Res = await fetch(
      `${baseUrl}/wallets/${wallet.id}/ledger?limit=2`,
    );
    expect(ledgerPage1Res.status).toBe(200);
    const page1 = (await ledgerPage1Res.json()) as {
      items: Array<{ id: string }>;
      nextCursor: string | null;
      hasMore: boolean;
    };
    expect(page1.items.length).toBe(2);
    expect(page1.hasMore).toBe(true);
    expect(page1.nextCursor).not.toBeNull();

    const ledgerPage2Res = await fetch(
      `${baseUrl}/wallets/${wallet.id}/ledger?cursor=${encodeURIComponent(page1.nextCursor!)}&limit=10`,
    );
    expect(ledgerPage2Res.status).toBe(200);
    const page2 = (await ledgerPage2Res.json()) as {
      items: Array<{ id: string }>;
      nextCursor: string | null;
      hasMore: boolean;
    };
    expect(page2.items.length).toBe(3);
    expect(page2.hasMore).toBe(false);
    expect(page2.nextCursor).toBeNull();

    // 12. POST /wallets/:walletId/reconciliation
    const reconRes = await fetch(
      `${baseUrl}/wallets/${wallet.id}/reconciliation`,
      { method: 'POST' },
    );
    expect(reconRes.status).toBe(200);
    const reconData = (await reconRes.json()) as {
      walletId: string;
      storedBalance: { amount: string; currency: string };
      calculatedBalance: { amount: string; currency: string };
      difference: { amount: string; currency: string };
      consistent: boolean;
      checkedEntries: number;
    };
    expect(reconData).toEqual({
      walletId: wallet.id,
      storedBalance: { amount: '1000.00', currency: 'BRL' },
      calculatedBalance: { amount: '1000.00', currency: 'BRL' },
      difference: { amount: '0.00', currency: 'BRL' },
      consistent: true,
      checkedEntries: 5,
    });
  });

  it('distinguishes REVERSAL_INSUFFICIENT_FUNDS from INSUFFICIENT_FUNDS (Section 7 Rule 9)', async () => {
    const playerId = randomUUID();
    const createRes = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: '50.00', currency: 'BRL' },
      }),
    });
    const wallet = (await createRes.json()) as { id: string };

    // 1. WIN of 100.00 BRL -> balance becomes 150.00 BRL
    const winExtId = `win-${randomUUID()}`;
    await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${winExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: winExtId,
        playerId,
        walletId: wallet.id,
        roundId: 'round-rev',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Win,
        money: { amount: '100.00', currency: 'BRL' },
      }),
    });

    // 2. Player bets 120.00 BRL in another round -> balance becomes 30.00 BRL
    const betExtId = `bet-${randomUUID()}`;
    await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${betExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: betExtId,
        playerId,
        walletId: wallet.id,
        roundId: 'round-rev-2',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Bet,
        money: { amount: '120.00', currency: 'BRL' },
      }),
    });

    // 3. Provider attempts to ROLLBACK the 100.00 BRL WIN while wallet only has 30.00 BRL
    // Must be REJECTED with REVERSAL_INSUFFICIENT_FUNDS (distinct from INSUFFICIENT_FUNDS)
    const rollbackExtId = `rollback-${randomUUID()}`;
    const rollbackRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${rollbackExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: rollbackExtId,
        playerId,
        walletId: wallet.id,
        roundId: 'round-rev',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Rollback,
        money: { amount: '100.00', currency: 'BRL' },
        referenceExternalTransactionId: winExtId,
      }),
    });
    expect(rollbackRes.status).toBe(422);
    const rollbackData = (await rollbackRes.json()) as {
      status: string;
      failureCode: string;
      balance: { amount: string; currency: string };
    };
    expect(rollbackData.status).toBe(WagerTransactionStatus.Rejected);
    expect(rollbackData.failureCode).toBe(FailureCode.ReversalInsufficientFunds);
    expect(rollbackData.balance).toEqual({ amount: '30.00', currency: 'BRL' });
  });

  it('processes SQS messages with Inbox deduplication, redelivery, retry, DLQ, and Outbox publishing + WebSocket events', async () => {
    resetQueues();
    wsGateway.clearHistory();

    const playerId = randomUUID();
    const createRes = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: '200.00', currency: 'BRL' },
      }),
    });
    const wallet = (await createRes.json()) as { id: string };

    // 1. Send valid WagerTransactionRequested to SQS
    const msgId = `msg-${randomUUID()}`;
    const extTxId = `sqs-bet-${randomUUID()}`;
    const sqsEnvelope = {
      messageId: msgId,
      type: 'WagerTransactionRequested',
      occurredAt: new Date().toISOString(),
      data: {
        providerId: 'provider-a',
        externalTransactionId: extTxId,
        idempotencyKey: `provider-a:${extTxId}`,
        playerId,
        walletId: wallet.id,
        roundId: 'round-sqs-1',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Bet,
        money: { amount: '50.00', currency: 'BRL' },
      },
    };

    await sqsClient.sendWagerMessage(sqsEnvelope, wallet.id, `${msgId}-1`);
    const processedCount = await sqsConsumer.pollOnce();
    expect(processedCount).toBe(1);

    // Redeliver the exact same messageId -> Inbox deduplicates without double debit
    await sqsClient.sendWagerMessage(sqsEnvelope, wallet.id, `${msgId}-2`);
    await sqsConsumer.pollOnce();

    const walletCheckRes = await fetch(`${baseUrl}/wallets/${wallet.id}`);
    const walletAfterSqs = (await walletCheckRes.json()) as {
      balance: { amount: string; currency: string };
    };
    expect(walletAfterSqs.balance).toEqual({ amount: '150.00', currency: 'BRL' });

    // 2. Test transient failure retry & DLQ routing after maxReceiveCount (3)
    const failingMsgId = `msg-fail-${randomUUID()}`;
    const failingEnvelope = {
      ...sqsEnvelope,
      messageId: failingMsgId,
      data: {
        ...sqsEnvelope.data,
        externalTransactionId: `sqs-fail-${randomUUID()}`,
        idempotencyKey: `provider-a:sqs-fail-${randomUUID()}`,
      },
    };

    await sqsClient.sendWagerMessage(failingEnvelope, wallet.id, failingMsgId);
    // Attempt 1 (transient failure -> visibility set to retry)
    const msgBatch1 = await sqsClient.receiveWagerMessages(1, 0);
    expect(msgBatch1.length).toBe(1);
    await sqsConsumer.handleSqsMessage(msgBatch1[0], { simulateTransientFailure: true });
    await sqsClient.changeVisibility(sqsClient.wagerQueueUrl, msgBatch1[0].ReceiptHandle!, 0);

    // Attempt 2
    const msgBatch2 = await sqsClient.receiveWagerMessages(1, 0);
    await sqsConsumer.handleSqsMessage(msgBatch2[0], { simulateTransientFailure: true });
    await sqsClient.changeVisibility(sqsClient.wagerQueueUrl, msgBatch2[0].ReceiptHandle!, 0);

    // Attempt 3 (reaches maxReceiveCount = 3 -> routed to DLQ and acked)
    const msgBatch3 = await sqsClient.receiveWagerMessages(1, 0);
    await sqsConsumer.handleSqsMessage(msgBatch3[0], { simulateTransientFailure: true });

    const dlqMessages = await sqsClient.receiveDlqMessages(10);
    expect(dlqMessages.length).toBeGreaterThanOrEqual(1);

    // 3. Run OutboxWorker and verify events are published to SQS AND emitted to WebSocket Gateway post-commit
    const outboxBatch = await outboxWorker.processBatch(100);
    expect(outboxBatch.published).toBeGreaterThan(0);

    const wsEvents = wsGateway.getRecentEmittedEvents();
    expect(wsEvents.some((e) => e.eventType === 'WalletBalanceChanged')).toBe(true);
    expect(wsEvents.some((e) => e.eventType === 'WagerTransactionProcessed')).toBe(true);
  });

  it('expires PENDING_REFERENCE transactions after retry limit with REFERENCE_NOT_FOUND and emits WagerTransactionRejected (Section 7.1)', async () => {
    const playerId = randomUUID();
    const createRes = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: '100.00', currency: 'BRL' },
      }),
    });
    const wallet = (await createRes.json()) as { id: string };

    // Submit REFUND referencing a non-existent BET
    const orphanRefundExtId = `orphan-refund-${randomUUID()}`;
    const refundRes = await fetch(`${baseUrl}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${orphanRefundExtId}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: orphanRefundExtId,
        playerId,
        walletId: wallet.id,
        roundId: 'round-orphan',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Refund,
        money: { amount: '25.00', currency: 'BRL' },
        referenceExternalTransactionId: 'missing-bet-forever',
      }),
    });
    expect(refundRes.status).toBe(202);
    const pendingData = (await refundRes.json()) as {
      transactionId: string;
      status: string;
    };
    expect(pendingData.status).toBe(WagerTransactionStatus.PendingReference);

    // Sweep 3 times with maxAttempts=3 -> on 3rd attempt transitions to REJECTED (REFERENCE_NOT_FOUND)
    await pendingRefWorker.sweepDuePendingReferences({
      maxAttempts: 3,
      baseDelayMs: 1,
      ignoreNextRetryAt: true,
    });
    await pendingRefWorker.sweepDuePendingReferences({
      maxAttempts: 3,
      baseDelayMs: 1,
      ignoreNextRetryAt: true,
    });
    const finalSweep = await pendingRefWorker.sweepDuePendingReferences({
      maxAttempts: 3,
      baseDelayMs: 1,
      ignoreNextRetryAt: true,
    });
    expect(finalSweep.rejected).toBeGreaterThanOrEqual(1);

    const txRes = await fetch(
      `${baseUrl}/wagering/transactions/${pendingData.transactionId}`,
    );
    const txAfterTimeout = (await txRes.json()) as {
      status: string;
      failureCode: string;
    };
    expect(txAfterTimeout.status).toBe(WagerTransactionStatus.Rejected);
    expect(txAfterTimeout.failureCode).toBe(FailureCode.ReferenceNotFound);
  });

  it('executes full CRUD for Providers, Games, and Wallets, plus Health & Metrics endpoints', async () => {
    // 1. Providers CRUD
    const providerId = `provider-crud-${randomUUID().slice(0, 8)}`;
    const createProvRes = await fetch(`${baseUrl}/providers`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: providerId,
        name: 'Jungle Studio CRUD',
        webhookUrl: 'https://hooks.junglegaming.io/callback',
        allowedCurrencies: ['BRL', 'USD'],
      }),
    });
    expect(createProvRes.status).toBe(201);

    const getProvRes = await fetch(`${baseUrl}/providers/${providerId}`);
    expect(getProvRes.status).toBe(200);

    const patchProvRes = await fetch(`${baseUrl}/providers/${providerId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'Jungle Studio Updated' }),
    });
    expect(patchProvRes.status).toBe(200);
    const patchedProv = (await patchProvRes.json()) as { name: string };
    expect(patchedProv.name).toBe('Jungle Studio Updated');

    // 2. Games CRUD
    const gameId = `game-crud-${randomUUID().slice(0, 8)}`;
    const createGameRes = await fetch(`${baseUrl}/games`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: gameId,
        providerId,
        name: 'Amazonia Crash',
        category: 'CRASH',
        minBet: { amount: '1.00', currency: 'BRL' },
        maxBet: { amount: '500.00', currency: 'BRL' },
      }),
    });
    expect(createGameRes.status).toBe(201);

    const patchGameRes = await fetch(`${baseUrl}/games/${gameId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'Amazonia Crash VIP' }),
    });
    expect(patchGameRes.status).toBe(200);

    const delGameRes = await fetch(`${baseUrl}/games/${gameId}`, {
      method: 'DELETE',
    });
    expect(delGameRes.status).toBe(200);

    const delProvRes = await fetch(`${baseUrl}/providers/${providerId}`, {
      method: 'DELETE',
    });
    expect(delProvRes.status).toBe(200);

    // 3. Wallets CRUD (GET list, PATCH metadata, DELETE zero-balance wallet)
    const zeroWalletRes = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId: randomUUID(),
        initialBalance: { amount: '0.00', currency: 'BRL' },
      }),
    });
    const zeroWallet = (await zeroWalletRes.json()) as { id: string };

    const patchWalletRes = await fetch(`${baseUrl}/wallets/${zeroWallet.id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ metadata: { tag: 'vip-test' } }),
    });
    expect(patchWalletRes.status).toBe(200);

    const closeWalletRes = await fetch(`${baseUrl}/wallets/${zeroWallet.id}`, {
      method: 'DELETE',
    });
    expect(closeWalletRes.status).toBe(200);
    const closedWallet = (await closeWalletRes.json()) as { status: string };
    expect(closedWallet.status).toBe('CLOSED');

    // 4. Root Dashboard, Health & Metrics endpoints
    const rootRes = await fetch(`${baseUrl}/`);
    expect(rootRes.status).toBe(200);
    const rootHtml = await rootRes.text();
    expect(rootHtml).toContain('Jungle Gaming');

    const liveRes = await fetch(`${baseUrl}/health/live`);
    expect(liveRes.status).toBe(200);

    const readyRes = await fetch(`${baseUrl}/health/ready`);
    expect(readyRes.status).toBe(200);

    const metricsPromRes = await fetch(`${baseUrl}/metrics`);
    expect(metricsPromRes.status).toBe(200);
    const promText = await metricsPromRes.text();
    expect(promText).toContain('wagering_transactions_total');

    const metricsJsonRes = await fetch(`${baseUrl}/metrics?format=json`);
    expect(metricsJsonRes.status).toBe(200);
    const metricsJson = (await metricsJsonRes.json()) as {
      duplicatesDetected: { total: number };
    };
    expect(metricsJson.duplicatesDetected.total).toBeGreaterThanOrEqual(1);
  });
});
