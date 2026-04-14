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
import { INITIAL_SCHEMA_UP_SQL } from '../../src/infrastructure/persistence/migrations/Migration20261105000001_InitialSchema';
import {
  ensureTestInfrastructure,
  shutdownTestInfrastructure,
  startNestTestServer,
} from '../helpers/test-infra';

describe('Concurrency — 8 Mandatory Real Parallelism Scenarios & Global Ledger Invariant (Sections 8 & 13)', () => {
  let node1: INestApplication;
  let node2: INestApplication;
  let node3: INestApplication;
  let url1: string;
  let url2: string;
  let url3: string;
  let resetQueues: () => void;
  const createdWalletIds: string[] = [];

  beforeAll(async () => {
    const infra = await ensureTestInfrastructure();
    resetQueues = infra.resetSqsQueues;

    // Spin up >= 3 simultaneous application instances sharing the same database and SQS broker
    node1 = await NestFactory.create(AppModule.register(), { logger: false });
    node2 = await NestFactory.create(AppModule.register(), { logger: false });
    node3 = await NestFactory.create(AppModule.register(), { logger: false });

    const em1 = node1.get(EntityManager);
    await em1.getConnection().execute(INITIAL_SCHEMA_UP_SQL);

    for (const instance of [node1, node2, node3]) {
      const sqs = instance.get(SqsClientService);
      sqs.refreshClient();
      await sqs.ensureQueuesExist();
    }

    url1 = await startNestTestServer(node1);
    url2 = await startNestTestServer(node2);
    url3 = await startNestTestServer(node3);
  });

  afterAll(async () => {
    for (const instance of [node1, node2, node3]) {
      if (instance) {
        try {
          await instance.close();
        } catch {
          // Ignore close errors if already closed in scenario 8
        }
      }
    }
    await shutdownTestInfrastructure();
  });

  async function createWallet(
    baseUrl: string,
    initialAmount: string,
    currency = 'BRL',
  ): Promise<{ id: string; playerId: string }> {
    const playerId = `player-${randomUUID()}`;
    const res = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: initialAmount, currency },
      }),
    });
    expect(res.status).toBe(201);
    const data = (await res.json()) as { id: string; playerId: string };
    createdWalletIds.push(data.id);
    return data;
  }

  async function assertWalletLedgerInvariant(
    baseUrl: string,
    walletId: string,
    expectedBalanceAmount?: string,
  ): Promise<void> {
    const reconRes = await fetch(`${baseUrl}/wallets/${walletId}/reconciliation`, {
      method: 'POST',
    });
    expect(reconRes.status).toBe(200);
    const recon = (await reconRes.json()) as {
      walletId: string;
      storedBalance: { amount: string; currency: string };
      calculatedBalance: { amount: string; currency: string };
      difference: { amount: string; currency: string };
      consistent: boolean;
      checkedEntries: number;
    };

    // Global Invariant (Section 13): wallet.balance == saldo reconstruído pelo ledger
    expect(recon.consistent).toBe(true);
    expect(recon.difference.amount).toBe('0.00');
    expect(recon.storedBalance).toEqual(recon.calculatedBalance);
    if (expectedBalanceAmount !== undefined) {
      expect(recon.storedBalance.amount).toBe(expectedBalanceAmount);
    }
  }

  it('Scenario 1: Same bet sent 50 times in parallel -> exactly one debit and 49 idempotent replays', async () => {
    const wallet = await createWallet(url1, '500.00');
    const extTxId = `bet-50x-${randomUUID()}`;
    const idemKey = `provider-a:${extTxId}`;
    const urls = [url1, url2, url3];

    const payload = {
      providerId: 'provider-a',
      externalTransactionId: extTxId,
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'round-50x',
      gameId: 'fortune-chimp',
      kind: WagerTransactionKind.Bet,
      money: { amount: '25.00', currency: 'BRL' },
    };

    const requests = Array.from({ length: 50 }, (_, idx) => {
      const targetUrl = urls[idx % urls.length];
      return fetch(`${targetUrl}/wagering/transactions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': idemKey,
        },
        body: JSON.stringify(payload),
      }).then(async (r) => ({
        status: r.status,
        body: (await r.json()) as {
          transactionId: string;
          status: string;
          balance: { amount: string; currency: string };
          idempotentReplay: boolean;
        },
      }));
    });

    const responses = await Promise.all(requests);

    // Every single response must succeed with 200 OK, status PROCESSED, and observed balance 475.00 BRL
    for (const res of responses) {
      expect(res.status).toBe(200);
      expect(res.body.status).toBe(WagerTransactionStatus.Processed);
      expect(res.body.balance).toEqual({ amount: '475.00', currency: 'BRL' });
    }

    const originalExecutions = responses.filter((r) => !r.body.idempotentReplay);
    const replays = responses.filter((r) => r.body.idempotentReplay);
    expect(originalExecutions.length).toBe(1);
    expect(replays.length).toBe(49);

    // Verify ledger has only 2 entries total (1 OPENING + 1 DEBIT)
    const ledgerRes = await fetch(`${url1}/wallets/${wallet.id}/ledger?limit=50`);
    const ledger = (await ledgerRes.json()) as {
      items: Array<{ direction: string; money: { amount: string } }>;
    };
    const debits = ledger.items.filter((i) => i.direction === 'DEBIT');
    expect(debits.length).toBe(1);
    expect(debits[0].money.amount).toBe('25.00');

    await assertWalletLedgerInvariant(url1, wallet.id, '475.00');
  });

  it('Scenario 2 (Section 8 Mandatory Scenario): Initial balance 100.00 BRL, two simultaneous 80.00 BRL bets -> 1 PROCESSED, 1 REJECTED, final balance 20.00 BRL, 1 DEBIT ledger entry', async () => {
    const wallet = await createWallet(url1, '100.00');

    const betAExt = `bet-80-a-${randomUUID()}`;
    const betBExt = `bet-80-b-${randomUUID()}`;

    const reqA = fetch(`${url1}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${betAExt}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: betAExt,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-race-80',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Bet,
        money: { amount: '80.00', currency: 'BRL' },
      }),
    }).then(async (r) => ({
      httpStatus: r.status,
      body: (await r.json()) as {
        status: string;
        failureCode?: string;
        balance: { amount: string; currency: string };
      },
    }));

    const reqB = fetch(`${url2}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${betBExt}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: betBExt,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-race-80',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Bet,
        money: { amount: '80.00', currency: 'BRL' },
      }),
    }).then(async (r) => ({
      httpStatus: r.status,
      body: (await r.json()) as {
        status: string;
        failureCode?: string;
        balance: { amount: string; currency: string };
      },
    }));

    const [resA, resB] = await Promise.all([reqA, reqB]);
    const results = [resA, resB];

    const processed = results.filter(
      (r) => r.body.status === WagerTransactionStatus.Processed,
    );
    const rejected = results.filter(
      (r) => r.body.status === WagerTransactionStatus.Rejected,
    );

    expect(processed.length).toBe(1);
    expect(rejected.length).toBe(1);
    expect(processed[0].httpStatus).toBe(200);
    expect(rejected[0].httpStatus).toBe(422);
    expect(rejected[0].body.failureCode).toBe(FailureCode.InsufficientFunds);

    // Verify exactly ONE debit entry in ledger
    const ledgerRes = await fetch(`${url1}/wallets/${wallet.id}/ledger?limit=50`);
    const ledger = (await ledgerRes.json()) as {
      items: Array<{ direction: string; money: { amount: string } }>;
    };
    const debits = ledger.items.filter((i) => i.direction === 'DEBIT');
    expect(debits.length).toBe(1);
    expect(debits[0].money.amount).toBe('80.00');

    // Verify retry of both does not duplicate debit
    const retryARes = await fetch(`${url3}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${betAExt}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: betAExt,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-race-80',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Bet,
        money: { amount: '80.00', currency: 'BRL' },
      }),
    });
    const retryABody = (await retryARes.json()) as { idempotentReplay: boolean };
    expect(retryABody.idempotentReplay).toBe(true);

    await assertWalletLedgerInvariant(url1, wallet.id, '20.00');
  });

  it('Scenario 3: Distinct wallets processed in parallel without cross-wallet contention', async () => {
    const wallets = await Promise.all(
      Array.from({ length: 8 }, () => createWallet(url1, '200.00')),
    );

    const parallelOps = wallets.flatMap((w, wIdx) => {
      const bet1Ext = `w${wIdx}-bet1-${randomUUID()}`;
      const bet2Ext = `w${wIdx}-bet2-${randomUUID()}`;
      const win1Ext = `w${wIdx}-win1-${randomUUID()}`;

      return [
        fetch(`${url1}/wagering/transactions`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Idempotency-Key': `provider-a:${bet1Ext}`,
          },
          body: JSON.stringify({
            providerId: 'provider-a',
            externalTransactionId: bet1Ext,
            playerId: w.playerId,
            walletId: w.id,
            roundId: `round-w-${wIdx}`,
            gameId: 'fortune-chimp',
            kind: WagerTransactionKind.Bet,
            money: { amount: '30.00', currency: 'BRL' },
          }),
        }),
        fetch(`${url2}/wagering/transactions`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Idempotency-Key': `provider-a:${bet2Ext}`,
          },
          body: JSON.stringify({
            providerId: 'provider-a',
            externalTransactionId: bet2Ext,
            playerId: w.playerId,
            walletId: w.id,
            roundId: `round-w-${wIdx}`,
            gameId: 'fortune-chimp',
            kind: WagerTransactionKind.Bet,
            money: { amount: '50.00', currency: 'BRL' },
          }),
        }),
        fetch(`${url3}/wagering/transactions`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Idempotency-Key': `provider-a:${win1Ext}`,
          },
          body: JSON.stringify({
            providerId: 'provider-a',
            externalTransactionId: win1Ext,
            playerId: w.playerId,
            walletId: w.id,
            roundId: `round-w-${wIdx}`,
            gameId: 'fortune-chimp',
            kind: WagerTransactionKind.Win,
            money: { amount: '15.00', currency: 'BRL' },
          }),
        }),
      ];
    });

    const responses = await Promise.all(parallelOps);
    for (const r of responses) {
      expect(r.status).toBe(200);
    }

    // Every wallet started at 200.00, -30.00, -50.00, +15.00 = 135.00 BRL
    for (const w of wallets) {
      await assertWalletLedgerInvariant(url1, w.id, '135.00');
    }
  });

  it('Scenario 4: >= 3 simultaneous application instances hitting the same hot wallet concurrently', async () => {
    const hotWallet = await createWallet(url1, '300.00');
    const instanceUrls = [url1, url2, url3];

    // 15 bets of 30.00 BRL across 3 instances (total requested = 450.00 BRL, available = 300.00 BRL)
    // Exactly 10 must be PROCESSED and 5 REJECTED (INSUFFICIENT_FUNDS), final balance = 0.00 BRL!
    const requests = Array.from({ length: 15 }, (_, idx) => {
      const targetUrl = instanceUrls[idx % instanceUrls.length];
      const extId = `hot-bet-${idx}-${randomUUID()}`;
      return fetch(`${targetUrl}/wagering/transactions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': `provider-a:${extId}`,
        },
        body: JSON.stringify({
          providerId: 'provider-a',
          externalTransactionId: extId,
          playerId: hotWallet.playerId,
          walletId: hotWallet.id,
          roundId: `round-hot-${idx}`,
          gameId: 'fortune-chimp',
          kind: WagerTransactionKind.Bet,
          money: { amount: '30.00', currency: 'BRL' },
        }),
      }).then(async (r) => (await r.json()) as { status: string; failureCode?: string });
    });

    const results = await Promise.all(requests);
    const processedCount = results.filter(
      (r) => r.status === WagerTransactionStatus.Processed,
    ).length;
    const rejectedCount = results.filter(
      (r) =>
        r.status === WagerTransactionStatus.Rejected &&
        r.failureCode === FailureCode.InsufficientFunds,
    ).length;

    expect(processedCount).toBe(10);
    expect(rejectedCount).toBe(5);

    await assertWalletLedgerInvariant(url1, hotWallet.id, '0.00');
  });

  it('Scenario 5: Worker crashes AFTER SQL commit and BEFORE SQS ack -> redelivery to another worker does not duplicate debit', async () => {
    resetQueues();
    const wallet = await createWallet(url1, '100.00');
    const sqsClient1 = node1.get(SqsClientService);
    const consumer1 = node1.get(SqsWagerConsumer);
    const consumer2 = node2.get(SqsWagerConsumer);

    const msgId = `msg-crash-${randomUUID()}`;
    const extTxId = `bet-crash-${randomUUID()}`;
    const envelope = {
      messageId: msgId,
      type: 'WagerTransactionRequested',
      occurredAt: new Date().toISOString(),
      data: {
        providerId: 'provider-a',
        externalTransactionId: extTxId,
        idempotencyKey: `provider-a:${extTxId}`,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-crash',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Bet,
        money: { amount: '40.00', currency: 'BRL' },
      },
    };

    await sqsClient1.sendWagerMessage(envelope, wallet.id, msgId);

    // Consumer 1 processes the message, commits SQL (balance -> 60.00), and crashes BEFORE ack!
    let crashedAfterCommit = false;
    try {
      await consumer1.pollOnce({ simulateCrashAfterCommitBeforeAck: true });
    } catch (err) {
      if ((err as Error)?.message === 'WORKER_CRASHED_AFTER_COMMIT_BEFORE_ACK') {
        crashedAfterCommit = true;
      }
    }
    expect(crashedAfterCommit).toBe(true);

    // Message is immediately redelivered to Consumer 2 on Instance 2
    const redeliveredCount = await consumer2.pollOnce();
    expect(redeliveredCount).toBe(1);

    // Balance must be 60.00 BRL (debited once, not twice!)
    await assertWalletLedgerInvariant(url2, wallet.id, '60.00');
  });

  it('Scenario 6: Two concurrent Outbox publishers running over the same outbox table', async () => {
    const outbox1 = node1.get(OutboxWorker);
    const outbox2 = node2.get(OutboxWorker);

    // Generate several transactions to populate outbox_messages
    const wallet = await createWallet(url1, '500.00');
    for (let i = 0; i < 6; i++) {
      const extId = `outbox-bet-${i}-${randomUUID()}`;
      await fetch(`${url1}/wagering/transactions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': `provider-a:${extId}`,
        },
        body: JSON.stringify({
          providerId: 'provider-a',
          externalTransactionId: extId,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: `round-outbox-${i}`,
          gameId: 'fortune-chimp',
          kind: WagerTransactionKind.Bet,
          money: { amount: '10.00', currency: 'BRL' },
        }),
      });
    }

    // Run both publishers concurrently until all pending outbox messages are published
    const [batchA, batchB, batchC] = await Promise.all([
      outbox1.processBatch(50),
      outbox2.processBatch(50),
      outbox1.processBatch(100),
    ]);

    // Verify no overlap in published message IDs between concurrent batches
    const setA = new Set(batchA.publishedIds);
    for (const id of batchB.publishedIds) {
      expect(setA.has(id)).toBe(false);
    }

    expect(batchA.published + batchB.published + batchC.published).toBeGreaterThanOrEqual(
      12,
    );

    // Verify all outbox messages are now published
    const em = node1.get(EntityManager);
    const pendingRows = (await em
      .getConnection()
      .execute(
        `SELECT COUNT(*)::int AS cnt FROM outbox_messages WHERE published_at IS NULL`,
      )) as Array<{ cnt: number }>;
    expect(Number(pendingRows[0].cnt)).toBe(0);
  });

  it('Scenario 7: REFUND and ROLLBACK delivered out-of-order BEFORE the referenced BET', async () => {
    const wallet = await createWallet(url1, '100.00');
    const pendingRefWorker = node2.get(PendingReferenceWorker);

    const lateBet1Ext = `late-bet-1-${randomUUID()}`;
    const earlyRefundExt = `early-refund-1-${randomUUID()}`;

    const lateBet2Ext = `late-bet-2-${randomUUID()}`;
    const earlyRollbackExt = `early-rollback-2-${randomUUID()}`;

    // 1. Deliver REFUND and ROLLBACK BEFORE their referenced BETs arrive!
    const [refundRes, rollbackRes] = await Promise.all([
      fetch(`${url1}/wagering/transactions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': `provider-a:${earlyRefundExt}`,
        },
        body: JSON.stringify({
          providerId: 'provider-a',
          externalTransactionId: earlyRefundExt,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'round-ooo-1',
          gameId: 'fortune-chimp',
          kind: WagerTransactionKind.Refund,
          money: { amount: '30.00', currency: 'BRL' },
          referenceExternalTransactionId: lateBet1Ext,
        }),
      }),
      fetch(`${url2}/wagering/transactions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': `provider-a:${earlyRollbackExt}`,
        },
        body: JSON.stringify({
          providerId: 'provider-a',
          externalTransactionId: earlyRollbackExt,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'round-ooo-2',
          gameId: 'fortune-chimp',
          kind: WagerTransactionKind.Rollback,
          money: { amount: '40.00', currency: 'BRL' },
          referenceExternalTransactionId: lateBet2Ext,
        }),
      }),
    ]);

    expect(refundRes.status).toBe(202);
    expect(rollbackRes.status).toBe(202);

    const refundPending = (await refundRes.json()) as {
      transactionId: string;
      status: string;
    };
    const rollbackPending = (await rollbackRes.json()) as {
      transactionId: string;
      status: string;
    };
    expect(refundPending.status).toBe(WagerTransactionStatus.PendingReference);
    expect(rollbackPending.status).toBe(WagerTransactionStatus.PendingReference);

    // Balance is still 100.00 BRL while waiting for references
    await assertWalletLedgerInvariant(url1, wallet.id, '100.00');

    // 2. Now the two referenced BETs (-30.00 and -40.00) arrive!
    const [bet1Res, bet2Res] = await Promise.all([
      fetch(`${url3}/wagering/transactions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': `provider-a:${lateBet1Ext}`,
        },
        body: JSON.stringify({
          providerId: 'provider-a',
          externalTransactionId: lateBet1Ext,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'round-ooo-1',
          gameId: 'fortune-chimp',
          kind: WagerTransactionKind.Bet,
          money: { amount: '30.00', currency: 'BRL' },
        }),
      }),
      fetch(`${url1}/wagering/transactions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': `provider-a:${lateBet2Ext}`,
        },
        body: JSON.stringify({
          providerId: 'provider-a',
          externalTransactionId: lateBet2Ext,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: 'round-ooo-2',
          gameId: 'fortune-chimp',
          kind: WagerTransactionKind.Bet,
          money: { amount: '40.00', currency: 'BRL' },
        }),
      }),
    ]);
    expect(bet1Res.status).toBe(200);
    expect(bet2Res.status).toBe(200);

    // Balance after the two BETs is 30.00 BRL
    await assertWalletLedgerInvariant(url1, wallet.id, '30.00');

    // 3. PendingReferenceWorker sweeps and processes both out-of-order reversals (+30.00 and +40.00 -> 100.00 BRL)
    const sweepResult = await pendingRefWorker.sweepDuePendingReferences({
      ignoreNextRetryAt: true,
    });
    expect(sweepResult.processed).toBe(2);

    const checkRefundRes = await fetch(
      `${url1}/wagering/transactions/${refundPending.transactionId}`,
    );
    const checkRefund = (await checkRefundRes.json()) as { status: string };
    expect(checkRefund.status).toBe(WagerTransactionStatus.Processed);

    await assertWalletLedgerInvariant(url1, wallet.id, '100.00');
  });

  it('Scenario 8: Service restart recovery & final invariant proof across ALL wallets (wallet.balance == ledger reconstructed balance)', async () => {
    const wallet = await createWallet(url3, '250.00');

    // Submit a bet on node3, then shut down node3 to simulate service restart
    const betExt = `restart-bet-${randomUUID()}`;
    const betRes = await fetch(`${url3}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${betExt}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: betExt,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-restart',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Bet,
        money: { amount: '50.00', currency: 'BRL' },
      }),
    });
    expect(betRes.status).toBe(200);

    // Graceful stop & restart node3
    await node3.close();
    node3 = await NestFactory.create(AppModule.register(), { logger: false });
    node3.get(SqsClientService).refreshClient();
    url3 = await startNestTestServer(node3);

    // Drain any remaining Outbox messages on the restarted node3
    const outbox3 = node3.get(OutboxWorker);
    await outbox3.processBatch(100);

    // Replay the pre-restart bet against the newly restarted node3 -> idempotent replay returns 200.00 BRL
    const replayAfterRestartRes = await fetch(`${url3}/wagering/transactions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `provider-a:${betExt}`,
      },
      body: JSON.stringify({
        providerId: 'provider-a',
        externalTransactionId: betExt,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'round-restart',
        gameId: 'fortune-chimp',
        kind: WagerTransactionKind.Bet,
        money: { amount: '50.00', currency: 'BRL' },
      }),
    });
    expect(replayAfterRestartRes.status).toBe(200);
    const replayData = (await replayAfterRestartRes.json()) as {
      idempotentReplay: boolean;
      balance: { amount: string; currency: string };
    };
    expect(replayData.idempotentReplay).toBe(true);
    expect(replayData.balance.amount).toBe('200.00');

    // Final Global Invariant Check across EVERY wallet created in all concurrency tests:
    // wallet.balance == saldo reconstruído pelo ledger
    expect(createdWalletIds.length).toBeGreaterThanOrEqual(14);
    for (const walletId of createdWalletIds) {
      await assertWalletLedgerInvariant(url3, walletId);
    }
  });
});
