import { Injectable } from '@nestjs/common';

export interface LatencyStats {
  count: number;
  minMs: number;
  maxMs: number;
  avgMs: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
}

export interface MetricsSnapshot {
  transactionsByStatus: Record<string, number>;
  transactionsByKindAndStatus: Record<string, number>;
  duplicatesDetected: {
    httpIdempotency: number;
    sqsInbox: number;
    total: number;
  };
  retries: {
    sqsConsumer: number;
    outboxPublisher: number;
    pendingReference: number;
    dbSerialization: number;
    total: number;
  };
  dlqMessages: number;
  lockConflicts: number;
  reconciliationDivergences: number;
  outboxLagSeconds: number;
  outboxPendingCount: number;
  processingLatencyMs: LatencyStats;
}

@Injectable()
export class MetricsService {
  private readonly transactionsByStatus = new Map<string, number>();
  private readonly transactionsByKindAndStatus = new Map<string, number>();
  private httpDuplicates = 0;
  private sqsInboxDuplicates = 0;
  private sqsRetries = 0;
  private outboxRetries = 0;
  private pendingRefRetries = 0;
  private dbSerializationRetries = 0;
  private dlqMessages = 0;
  private lockConflicts = 0;
  private reconciliationDivergences = 0;
  private outboxLagSeconds = 0;
  private outboxPendingCount = 0;
  private readonly latencySamples: number[] = [];
  private readonly maxLatencySamples = 5000;

  incrementTransactionStatus(status: string, kind = 'UNKNOWN'): void {
    this.transactionsByStatus.set(
      status,
      (this.transactionsByStatus.get(status) ?? 0) + 1,
    );
    const compositeKey = `${kind}:${status}`;
    this.transactionsByKindAndStatus.set(
      compositeKey,
      (this.transactionsByKindAndStatus.get(compositeKey) ?? 0) + 1,
    );
  }

  incrementDuplicateDetected(source: 'http' | 'sqs'): void {
    if (source === 'http') {
      this.httpDuplicates += 1;
    } else {
      this.sqsInboxDuplicates += 1;
    }
  }

  incrementRetry(
    component: 'sqs_consumer' | 'outbox_publisher' | 'pending_reference' | 'db_serialization',
  ): void {
    switch (component) {
      case 'sqs_consumer':
        this.sqsRetries += 1;
        break;
      case 'outbox_publisher':
        this.outboxRetries += 1;
        break;
      case 'pending_reference':
        this.pendingRefRetries += 1;
        break;
      case 'db_serialization':
        this.dbSerializationRetries += 1;
        break;
    }
  }

  incrementDlqMessages(): void {
    this.dlqMessages += 1;
  }

  incrementLockConflict(): void {
    this.lockConflicts += 1;
  }

  incrementReconciliationDivergence(): void {
    this.reconciliationDivergences += 1;
  }

  setOutboxMetrics(lagSeconds: number, pendingCount: number): void {
    this.outboxLagSeconds = Math.max(0, Number(lagSeconds.toFixed(3)));
    this.outboxPendingCount = Math.max(0, pendingCount);
  }

  recordProcessingLatency(durationMs: number): void {
    if (!Number.isFinite(durationMs) || durationMs < 0) {
      return;
    }
    this.latencySamples.push(durationMs);
    if (this.latencySamples.length > this.maxLatencySamples) {
      this.latencySamples.shift();
    }
  }

  getSnapshot(): MetricsSnapshot {
    const txByStatus: Record<string, number> = {};
    for (const [k, v] of this.transactionsByStatus.entries()) {
      txByStatus[k] = v;
    }
    const txByKindAndStatus: Record<string, number> = {};
    for (const [k, v] of this.transactionsByKindAndStatus.entries()) {
      txByKindAndStatus[k] = v;
    }

    return {
      transactionsByStatus: txByStatus,
      transactionsByKindAndStatus: txByKindAndStatus,
      duplicatesDetected: {
        httpIdempotency: this.httpDuplicates,
        sqsInbox: this.sqsInboxDuplicates,
        total: this.httpDuplicates + this.sqsInboxDuplicates,
      },
      retries: {
        sqsConsumer: this.sqsRetries,
        outboxPublisher: this.outboxRetries,
        pendingReference: this.pendingRefRetries,
        dbSerialization: this.dbSerializationRetries,
        total:
          this.sqsRetries +
          this.outboxRetries +
          this.pendingRefRetries +
          this.dbSerializationRetries,
      },
      dlqMessages: this.dlqMessages,
      lockConflicts: this.lockConflicts,
      reconciliationDivergences: this.reconciliationDivergences,
      outboxLagSeconds: this.outboxLagSeconds,
      outboxPendingCount: this.outboxPendingCount,
      processingLatencyMs: this.computeLatencyStats(),
    };
  }

  toPrometheusText(): string {
    const snap = this.getSnapshot();
    const lines: string[] = [
      '# HELP wagering_transactions_total Total wagering transactions by status',
      '# TYPE wagering_transactions_total counter',
    ];

    for (const [status, count] of Object.entries(snap.transactionsByStatus)) {
      lines.push(`wagering_transactions_total{status="${status}"} ${count}`);
    }

    lines.push(
      '# HELP wagering_duplicates_detected_total Total duplicate operations detected',
      '# TYPE wagering_duplicates_detected_total counter',
      `wagering_duplicates_detected_total{source="http"} ${snap.duplicatesDetected.httpIdempotency}`,
      `wagering_duplicates_detected_total{source="sqs"} ${snap.duplicatesDetected.sqsInbox}`,
      '# HELP wagering_retries_total Total retry attempts by component',
      '# TYPE wagering_retries_total counter',
      `wagering_retries_total{component="sqs_consumer"} ${snap.retries.sqsConsumer}`,
      `wagering_retries_total{component="outbox_publisher"} ${snap.retries.outboxPublisher}`,
      `wagering_retries_total{component="pending_reference"} ${snap.retries.pendingReference}`,
      `wagering_retries_total{component="db_serialization"} ${snap.retries.dbSerialization}`,
      '# HELP wagering_dlq_messages_total Total messages routed to DLQ',
      '# TYPE wagering_dlq_messages_total counter',
      `wagering_dlq_messages_total ${snap.dlqMessages}`,
      '# HELP wagering_lock_conflicts_total Total concurrency lock conflicts',
      '# TYPE wagering_lock_conflicts_total counter',
      `wagering_lock_conflicts_total ${snap.lockConflicts}`,
      '# HELP wagering_reconciliation_divergences_total Total ledger reconciliation divergences',
      '# TYPE wagering_reconciliation_divergences_total counter',
      `wagering_reconciliation_divergences_total ${snap.reconciliationDivergences}`,
      '# HELP wagering_outbox_lag_seconds Oldest unpublished outbox message age in seconds',
      '# TYPE wagering_outbox_lag_seconds gauge',
      `wagering_outbox_lag_seconds ${snap.outboxLagSeconds}`,
      '# HELP wagering_outbox_pending_count Pending unpublished outbox messages',
      '# TYPE wagering_outbox_pending_count gauge',
      `wagering_outbox_pending_count ${snap.outboxPendingCount}`,
      '# HELP wagering_processing_duration_ms Processing latency percentiles in milliseconds',
      '# TYPE wagering_processing_duration_ms summary',
      `wagering_processing_duration_ms{quantile="0.5"} ${snap.processingLatencyMs.p50Ms}`,
      `wagering_processing_duration_ms{quantile="0.95"} ${snap.processingLatencyMs.p95Ms}`,
      `wagering_processing_duration_ms{quantile="0.99"} ${snap.processingLatencyMs.p99Ms}`,
      `wagering_processing_duration_ms_count ${snap.processingLatencyMs.count}`,
    );

    return `${lines.join('\n')}\n`;
  }

  reset(): void {
    this.transactionsByStatus.clear();
    this.transactionsByKindAndStatus.clear();
    this.httpDuplicates = 0;
    this.sqsInboxDuplicates = 0;
    this.sqsRetries = 0;
    this.outboxRetries = 0;
    this.pendingRefRetries = 0;
    this.dbSerializationRetries = 0;
    this.dlqMessages = 0;
    this.lockConflicts = 0;
    this.reconciliationDivergences = 0;
    this.outboxLagSeconds = 0;
    this.outboxPendingCount = 0;
    this.latencySamples.length = 0;
  }

  private computeLatencyStats(): LatencyStats {
    if (this.latencySamples.length === 0) {
      return {
        count: 0,
        minMs: 0,
        maxMs: 0,
        avgMs: 0,
        p50Ms: 0,
        p95Ms: 0,
        p99Ms: 0,
      };
    }

    const sorted = [...this.latencySamples].sort((a, b) => a - b);
    const count = sorted.length;
    const sum = sorted.reduce((acc, v) => acc + v, 0);
    const percentile = (p: number): number => {
      const idx = Math.min(count - 1, Math.max(0, Math.ceil((p / 100) * count) - 1));
      return Number(sorted[idx].toFixed(2));
    };

    return {
      count,
      minMs: Number(sorted[0].toFixed(2)),
      maxMs: Number(sorted[count - 1].toFixed(2)),
      avgMs: Number((sum / count).toFixed(2)),
      p50Ms: percentile(50),
      p95Ms: percentile(95),
      p99Ms: percentile(99),
    };
  }
}
