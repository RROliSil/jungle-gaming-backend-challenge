import { randomUUID } from 'crypto';
import * as os from 'os';
import { EntityManager } from '@mikro-orm/core';
import { NestFactory } from '@nestjs/core';
import { AppModule } from '../src/app.module';
import { WagerTransactionKind } from '../src/domain/wagering/wager-transaction';
import { OutboxWorker } from '../src/infrastructure/messaging/outbox.worker';
import { SqsClientService } from '../src/infrastructure/messaging/sqs-client.service';
import { MetricsService } from '../src/infrastructure/observability/metrics.service';
import { INITIAL_SCHEMA_UP_SQL } from '../src/infrastructure/persistence/migrations/Migration20261105000001_InitialSchema';
import {
  ensureTestInfrastructure,
  shutdownTestInfrastructure,
  startNestTestServer,
} from '../test/helpers/test-infra';

async function runLoadTest(): Promise<void> {
  process.env.LOG_SILENT = 'true';
  const infra = await ensureTestInfrastructure();

  const app = await NestFactory.create(AppModule.register(), { logger: false });
  const em = app.get(EntityManager);
  await em.getConnection().execute(INITIAL_SCHEMA_UP_SQL);

  const sqs = app.get(SqsClientService);
  sqs.refreshClient();
  await sqs.ensureQueuesExist();

  const outboxWorker = app.get(OutboxWorker);
  const metricsService = app.get(MetricsService);
  metricsService.reset();

  const baseUrl = await startNestTestServer(app);

  const walletCount = 10;
  const operationsPerWallet = 20;
  const hotWalletContentionRequests = 40;
  const wallets: Array<{ id: string; playerId: string }> = [];

  for (let i = 0; i < walletCount; i++) {
    const playerId = `load-player-${i}-${randomUUID()}`;
    const res = await fetch(`${baseUrl}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId,
        initialBalance: { amount: '1000.00', currency: 'BRL' },
      }),
    });
    const w = (await res.json()) as { id: string; playerId: string };
    wallets.push(w);
  }

  const latenciesMs: number[] = [];
  let totalRequests = 0;
  let http2xx = 0;
  let http422BusinessRejected = 0;
  let http5xxErrors = 0;

  const suiteStart = performance.now();

  // Phase 1: Parallel multi-wallet mixed workload (BET + WIN + Idempotent Replays)
  const phase1Promises: Promise<void>[] = [];
  for (const w of wallets) {
    for (let op = 0; op < operationsPerWallet; op++) {
      phase1Promises.push(
        (async () => {
          const isWin = op % 4 === 3;
          const extId = `load-${w.id.slice(0, 6)}-${op}-${randomUUID()}`;
          const idemKey = `provider-a:${extId}`;
          const payload = {
            providerId: 'provider-a',
            externalTransactionId: extId,
            playerId: w.playerId,
            walletId: w.id,
            roundId: `round-${op}`,
            gameId: 'fortune-chimp',
            kind: isWin ? WagerTransactionKind.Win : WagerTransactionKind.Bet,
            money: { amount: isWin ? '40.00' : '15.00', currency: 'BRL' },
          };

          const t0 = performance.now();
          const res = await fetch(`${baseUrl}/wagering/transactions`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Idempotency-Key': idemKey,
            },
            body: JSON.stringify(payload),
          });
          latenciesMs.push(performance.now() - t0);
          totalRequests++;

          if (res.status >= 200 && res.status < 300) {
            http2xx++;
          } else if (res.status === 422) {
            http422BusinessRejected++;
          } else if (res.status >= 500) {
            http5xxErrors++;
          }

          // 20% chance of immediate idempotent replay
          if (op % 5 === 0) {
            const tReplay = performance.now();
            const replayRes = await fetch(`${baseUrl}/wagering/transactions`, {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
                'Idempotency-Key': idemKey,
              },
              body: JSON.stringify(payload),
            });
            latenciesMs.push(performance.now() - tReplay);
            totalRequests++;
            if (replayRes.status === 200) {
              http2xx++;
            }
          }
        })(),
      );
    }
  }

  await Promise.all(phase1Promises);

  // Phase 2: Hot-wallet high-contention burst (40 simultaneous bets against 1 wallet)
  const hotWallet = wallets[0];
  const hotPromises = Array.from({ length: hotWalletContentionRequests }, (_, idx) =>
    (async () => {
      const extId = `hot-burst-${idx}-${randomUUID()}`;
      const t0 = performance.now();
      const res = await fetch(`${baseUrl}/wagering/transactions`, {
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
          roundId: `round-hot-burst-${idx}`,
          gameId: 'fortune-chimp',
          kind: WagerTransactionKind.Bet,
          money: { amount: '50.00', currency: 'BRL' },
        }),
      });
      latenciesMs.push(performance.now() - t0);
      totalRequests++;
      if (res.status === 200) {
        http2xx++;
      } else if (res.status === 422) {
        http422BusinessRejected++;
      } else if (res.status >= 500) {
        http5xxErrors++;
      }
    })(),
  );

  await Promise.all(hotPromises);

  const totalDurationSec = (performance.now() - suiteStart) / 1000;

  // Measure Outbox lag before & after draining
  await outboxWorker.refreshOutboxLagMetrics();
  const preDrainSnap = metricsService.getSnapshot();

  const drainStart = performance.now();
  let totalOutboxPublished = 0;
  while (true) {
    const batch = await outboxWorker.processBatch(200);
    totalOutboxPublished += batch.published;
    if (batch.claimed === 0) {
      break;
    }
  }
  const outboxDrainDurationMs = Number((performance.now() - drainStart).toFixed(2));
  await outboxWorker.refreshOutboxLagMetrics();
  const postDrainSnap = metricsService.getSnapshot();

  // Verify ledger reconciliation invariant across all 10 wallets
  let allConsistent = true;
  let totalCheckedLedgerEntries = 0;
  for (const w of wallets) {
    const reconRes = await fetch(`${baseUrl}/wallets/${w.id}/reconciliation`, {
      method: 'POST',
    });
    const recon = (await reconRes.json()) as {
      consistent: boolean;
      checkedEntries: number;
    };
    if (!recon.consistent) {
      allConsistent = false;
    }
    totalCheckedLedgerEntries += recon.checkedEntries;
  }

  latenciesMs.sort((a, b) => a - b);
  const pct = (p: number) =>
    Number(
      latenciesMs[
        Math.min(
          latenciesMs.length - 1,
          Math.max(0, Math.ceil((p / 100) * latenciesMs.length) - 1),
        )
      ].toFixed(2),
    );

  const report = {
    environment: {
      os: `${os.platform()} ${os.release()} (${os.arch()})`,
      cpuCores: os.cpus().length,
      cpuModel: os.cpus()[0]?.model ?? 'Unknown CPU',
      runtime: `Bun ${process.versions.bun ?? process.version}`,
      databaseMode: infra.usingExternalPostgres
        ? 'PostgreSQL 16 (Docker TCP)'
        : 'PostgreSQL 16 (Embedded PGlite WASM)',
      sqsMode: infra.usingExternalSqs
        ? 'AWS SQS LocalStack (Docker TCP)'
        : 'AWS SQS HTTP Local Broker',
    },
    methodology: {
      walletsCreated: walletCount,
      operationsPerWallet,
      idempotentReplaysInjected: walletCount * (operationsPerWallet / 5),
      hotWalletContentionBurst: hotWalletContentionRequests,
      totalHttpRequests: totalRequests,
    },
    results: {
      totalDurationSeconds: Number(totalDurationSec.toFixed(3)),
      throughputRps: Number((totalRequests / totalDurationSec).toFixed(2)),
      latencyMs: {
        min: Number(latenciesMs[0].toFixed(2)),
        p50: pct(50),
        p95: pct(95),
        p99: pct(99),
        max: Number(latenciesMs[latenciesMs.length - 1].toFixed(2)),
      },
      statusBreakdown: {
        http2xxProcessedOrReplay: http2xx,
        http422BusinessRejected: http422BusinessRejected,
        http5xxInfrastructureErrors: http5xxErrors,
        unexpectedErrorRatePercent: Number(
          ((http5xxErrors / totalRequests) * 100).toFixed(2),
        ),
      },
      concurrencyAndOutbox: {
        lockConflictsDetected: postDrainSnap.lockConflicts,
        duplicatesDetected: postDrainSnap.duplicatesDetected.total,
        peakOutboxPendingBeforeDrain: preDrainSnap.outboxPendingCount,
        peakOutboxLagSecondsBeforeDrain: preDrainSnap.outboxLagSeconds,
        totalOutboxEventsPublished: totalOutboxPublished,
        outboxDrainDurationMs,
        postDrainOutboxPending: postDrainSnap.outboxPendingCount,
      },
      globalInvariantVerification: {
        allWalletsReconciledConsistent: allConsistent,
        totalLedgerEntriesVerified: totalCheckedLedgerEntries,
      },
    },
  };

  console.log('\n============================================================');
  console.log('🦧 JUNGLE GAMING — DISTRIBUTED WAGERING LOAD TEST REPORT');
  console.log('============================================================');
  console.log(JSON.stringify(report, null, 2));
  console.log('============================================================\n');

  await app.close();
  await shutdownTestInfrastructure();

  if (!allConsistent || http5xxErrors > 0) {
    process.exit(1);
  }
}

void runLoadTest();
