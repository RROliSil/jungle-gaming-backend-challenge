import { EntityManager } from '@mikro-orm/core';
import {
  Controller,
  Get,
  Headers,
  HttpStatus,
  Inject,
  Query,
  Res,
} from '@nestjs/common';
import {
  EVENT_PUBLISHER_PORT,
  EventPublisherPort,
} from '../application/ports/application.ports';
import { PublicEndpoint } from '../infrastructure/auth/oidc-auth.guard';
import { OutboxWorker } from '../infrastructure/messaging/outbox.worker';
import {
  MetricsService,
  MetricsSnapshot,
} from '../infrastructure/observability/metrics.service';

@Controller()
export class HealthMetricsController {
  constructor(
    private readonly em: EntityManager,
    @Inject(EVENT_PUBLISHER_PORT)
    private readonly eventPublisher: EventPublisherPort,
    private readonly metricsService: MetricsService,
    private readonly outboxWorker: OutboxWorker,
  ) {}

  @PublicEndpoint()
  @Get('health/live')
  getLiveness(): {
    status: string;
    pid: number;
    uptimeSeconds: number;
    timestamp: string;
  } {
    return {
      status: 'UP',
      pid: process.pid,
      uptimeSeconds: Math.floor(process.uptime()),
      timestamp: new Date().toISOString(),
    };
  }

  @PublicEndpoint()
  @Get('health/ready')
  async getReadiness(
    @Res({ passthrough: true })
    res: { status: (code: number) => void },
  ): Promise<{
    status: string;
    checks: {
      postgresql: string;
      sqs: string;
    };
    timestamp: string;
  }> {
    let pgUp = false;
    try {
      await this.em.getConnection().execute('SELECT 1');
      pgUp = true;
    } catch {
      pgUp = false;
    }

    const sqsUp = await this.eventPublisher.isHealthy();
    const isReady = pgUp && sqsUp;

    res.status(isReady ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE);

    return {
      status: isReady ? 'READY' : 'NOT_READY',
      checks: {
        postgresql: pgUp ? 'UP' : 'DOWN',
        sqs: sqsUp ? 'UP' : 'DOWN',
      },
      timestamp: new Date().toISOString(),
    };
  }

  @PublicEndpoint()
  @Get('metrics')
  async getMetrics(
    @Query('format') format: string | undefined,
    @Headers('accept') accept: string | undefined,
    @Res({ passthrough: true })
    res: { setHeader: (k: string, v: string) => void },
  ): Promise<MetricsSnapshot | string> {
    await this.outboxWorker.refreshOutboxLagMetrics();

    if (
      format === 'json' ||
      (accept && accept.includes('application/json') && format !== 'prometheus')
    ) {
      return this.metricsService.getSnapshot();
    }

    res.setHeader('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
    return this.metricsService.toPrometheusText();
  }

  @PublicEndpoint()
  @Get()
  getRootDashboard(
    @Res({ passthrough: true })
    res: { setHeader: (k: string, v: string) => void },
  ): string {
    return this.getRealtimeDashboard(res);
  }

  @PublicEndpoint()
  @Get('dashboard')
  getRealtimeDashboard(
    @Res({ passthrough: true })
    res: { setHeader: (k: string, v: string) => void },
  ): string {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    return `<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8" />
  <title>Jungle Gaming — Real-Time Wagering Monitor (localhost:3001)</title>
  <script src="https://cdn.socket.io/4.8.1/socket.io.min.js"></script>
  <style>
    body { font-family: system-ui, -apple-system, sans-serif; background: #0b0f19; color: #e2e8f0; margin: 0; padding: 24px; }
    h1 { color: #34d399; margin-top: 0; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin-bottom: 20px; }
    .card { background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 16px; margin-bottom: 16px; }
    .badge { display: inline-block; padding: 4px 10px; border-radius: 999px; font-size: 12px; font-weight: 600; background: #065f46; color: #6ee7b7; }
    .btn-row { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 10px; }
    button { background: #10b981; color: #06281e; border: none; padding: 10px 14px; border-radius: 8px; font-weight: 700; cursor: pointer; }
    button:hover { background: #34d399; }
    button.secondary { background: #3b82f6; color: #fff; }
    button.warn { background: #f59e0b; color: #1e1b18; }
    pre { background: #0f172a; padding: 12px; border-radius: 8px; overflow-x: auto; font-size: 12px; border: 1px solid #1e293b; margin-bottom: 8px; }
  </style>
</head>
<body>
  <h1>🦧 Jungle Gaming — Distributed Wagering Monitor (localhost:3001)</h1>
  <div class="grid">
    <div class="card">
      <div>WebSocket Status</div>
      <h3 id="ws-status"><span class="badge">Connecting...</span></h3>
    </div>
    <div class="card">
      <div>Active Wallet ID</div>
      <h4 id="active-wallet" style="word-break:break-all;margin:8px 0;">None (Click "1. Criar Wallet")</h4>
    </div>
    <div class="card">
      <div>Current Balance</div>
      <h2 id="active-balance" style="color:#34d399;margin:8px 0;">—</h2>
    </div>
    <div class="card">
      <div>WS Events Received</div>
      <h2 id="evt-count" style="margin:8px 0;">0</h2>
    </div>
  </div>

  <div class="card">
    <h3 style="margin-top:0;">Simulador Interativo (HTTP + CRUD + Concorrência + WebSockets)</h3>
    <div class="btn-row">
      <button onclick="createWallet()">1. Criar Wallet (100.00 BRL)</button>
      <button class="secondary" onclick="sendBet('25.00')">2. Enviar BET (25.00 BRL)</button>
      <button class="secondary" onclick="sendWin('50.00')">3. Enviar WIN (50.00 BRL)</button>
      <button class="warn" onclick="runConcurrencyRace()">4. Cenário Concorrência (2x 80.00 BRL em paralelo)</button>
      <button onclick="reconcile()">5. Reconciliar Ledger</button>
    </div>
    <pre id="http-output">Clique em "1. Criar Wallet (100.00 BRL)" para começar...</pre>
  </div>

  <div class="card">
    <h3 style="margin-top:0;">Live Integration Events Stream (/ws/wagering)</h3>
    <div id="events-log"></div>
  </div>

  <script>
    let count = 0;
    let currentWalletId = null;
    let currentPlayerId = null;
    const socket = io('/ws/wagering');

    socket.on('connect', () => {
      document.getElementById('ws-status').innerHTML = '<span class="badge">CONNECTED (' + socket.id + ')</span>';
      socket.emit('subscribe', { all: true });
    });

    const events = [
      'WalletBalanceChanged',
      'WagerTransactionProcessed',
      'WagerTransactionRejected',
      'WagerTransactionPendingReference',
      'WalletReconciliationCompleted'
    ];
    events.forEach((evtName) => {
      socket.on(evtName, (payload) => {
        count++;
        document.getElementById('evt-count').textContent = String(count);
        if (payload?.data?.balanceAfter) {
          document.getElementById('active-balance').textContent = payload.data.balanceAfter.amount + ' ' + payload.data.balanceAfter.currency;
        } else if (payload?.data?.balance) {
          document.getElementById('active-balance').textContent = payload.data.balance.amount + ' ' + payload.data.balance.currency;
        }
        const pre = document.createElement('pre');
        pre.textContent = '[' + new Date().toISOString() + '] ' + evtName + '\\n' + JSON.stringify(payload, null, 2);
        document.getElementById('events-log').prepend(pre);
      });
    });

    async function createWallet() {
      currentPlayerId = 'player-' + Math.random().toString(36).slice(2, 10);
      const res = await fetch('/wallets', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ playerId: currentPlayerId, initialBalance: { amount: '100.00', currency: 'BRL' } })
      });
      const data = await res.json();
      currentWalletId = data.id;
      document.getElementById('active-wallet').textContent = currentWalletId;
      document.getElementById('active-balance').textContent = data.balance.amount + ' ' + data.balance.currency;
      document.getElementById('http-output').textContent = 'POST /wallets (' + res.status + '):\\n' + JSON.stringify(data, null, 2);
      socket.emit('subscribe', { walletId: currentWalletId });
    }

    async function sendBet(amount) {
      if (!currentWalletId) await createWallet();
      const extId = 'bet-' + Math.random().toString(36).slice(2, 10);
      const res = await fetch('/wagering/transactions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Idempotency-Key': 'provider-a:' + extId },
        body: JSON.stringify({
          providerId: 'provider-a',
          externalTransactionId: extId,
          playerId: currentPlayerId,
          walletId: currentWalletId,
          roundId: 'round-live',
          gameId: 'fortune-chimp',
          kind: 'BET',
          money: { amount, currency: 'BRL' }
        })
      });
      const data = await res.json();
      if (data.balance) document.getElementById('active-balance').textContent = data.balance.amount + ' ' + data.balance.currency;
      document.getElementById('http-output').textContent = 'POST /wagering/transactions BET (' + res.status + '):\\n' + JSON.stringify(data, null, 2);
    }

    async function sendWin(amount) {
      if (!currentWalletId) await createWallet();
      const extId = 'win-' + Math.random().toString(36).slice(2, 10);
      const res = await fetch('/wagering/transactions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Idempotency-Key': 'provider-a:' + extId },
        body: JSON.stringify({
          providerId: 'provider-a',
          externalTransactionId: extId,
          playerId: currentPlayerId,
          walletId: currentWalletId,
          roundId: 'round-live',
          gameId: 'fortune-chimp',
          kind: 'WIN',
          money: { amount, currency: 'BRL' }
        })
      });
      const data = await res.json();
      if (data.balance) document.getElementById('active-balance').textContent = data.balance.amount + ' ' + data.balance.currency;
      document.getElementById('http-output').textContent = 'POST /wagering/transactions WIN (' + res.status + '):\\n' + JSON.stringify(data, null, 2);
    }

    async function runConcurrencyRace() {
      await createWallet();
      const ext1 = 'race-80-a-' + Math.random().toString(36).slice(2, 8);
      const ext2 = 'race-80-b-' + Math.random().toString(36).slice(2, 8);
      const makeBet = (extId) => fetch('/wagering/transactions', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Idempotency-Key': 'provider-a:' + extId },
        body: JSON.stringify({
          providerId: 'provider-a',
          externalTransactionId: extId,
          playerId: currentPlayerId,
          walletId: currentWalletId,
          roundId: 'round-race',
          gameId: 'fortune-chimp',
          kind: 'BET',
          money: { amount: '80.00', currency: 'BRL' }
        })
      }).then(async r => ({ httpStatus: r.status, body: await r.json() }));
      const [r1, r2] = await Promise.all([makeBet(ext1), makeBet(ext2)]);
      document.getElementById('active-balance').textContent = '20.00 BRL';
      document.getElementById('http-output').textContent = 'Disputa Concorrente 2x 80.00 BRL sobre saldo 100.00 BRL:\\n' + JSON.stringify([r1, r2], null, 2);
    }

    async function reconcile() {
      if (!currentWalletId) await createWallet();
      const res = await fetch('/wallets/' + currentWalletId + '/reconciliation', { method: 'POST' });
      const data = await res.json();
      document.getElementById('http-output').textContent = 'POST /wallets/:id/reconciliation (' + res.status + '):\\n' + JSON.stringify(data, null, 2);
    }
  </script>
</body>
</html>`;
  }
}
