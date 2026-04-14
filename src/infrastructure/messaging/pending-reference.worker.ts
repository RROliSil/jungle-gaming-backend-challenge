import { EntityManager } from '@mikro-orm/core';
import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ProcessWagerTransactionUseCase } from '../../application/use-cases/process-wager-transaction.use-case';
import { WagerTransactionStatus } from '../../domain/wagering/wager-transaction';
import { StructuredLogger } from '../observability/structured-logger';

export interface PendingReferenceSweepResult {
  checked: number;
  processed: number;
  rejected: number;
  stillPending: number;
}

@Injectable()
export class PendingReferenceWorker implements OnModuleInit, OnModuleDestroy {
  private timer?: ReturnType<typeof setTimeout>;
  private isShuttingDown = false;

  constructor(
    private readonly em: EntityManager,
    private readonly processWagerUseCase: ProcessWagerTransactionUseCase,
    private readonly logger: StructuredLogger,
  ) {}

  onModuleInit(): void {
    if (process.env.PENDING_REF_WORKER_DISABLED === 'true') {
      return;
    }
    this.scheduleNextTick();
  }

  onModuleDestroy(): void {
    this.isShuttingDown = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
  }

  /**
   * Sweeps due PENDING_REFERENCE transactions using `FOR UPDATE SKIP LOCKED`
   * and reprocesses them with exponential backoff (Section 7.1).
   */
  async sweepDuePendingReferences(
    options?: {
      batchSize?: number;
      maxAttempts?: number;
      baseDelayMs?: number;
      ignoreNextRetryAt?: boolean;
    },
  ): Promise<PendingReferenceSweepResult> {
    const batchSize = options?.batchSize ?? 25;
    const maxAttempts =
      options?.maxAttempts ?? Number(process.env.PENDING_REF_MAX_ATTEMPTS ?? 5);
    const baseDelayMs =
      options?.baseDelayMs ?? Number(process.env.PENDING_REF_BASE_DELAY_MS ?? 100);

    const forkEm = this.em.fork();
    const conn = forkEm.getConnection();
    const nowIso = new Date().toISOString();

    const query = options?.ignoreNextRetryAt
      ? `SELECT id
         FROM wager_transactions
         WHERE status = 'PENDING_REFERENCE'
         ORDER BY created_at ASC
         LIMIT ?`
      : `SELECT id
         FROM wager_transactions
         WHERE status = 'PENDING_REFERENCE'
           AND (next_retry_at IS NULL OR next_retry_at <= ?)
         ORDER BY created_at ASC
         LIMIT ?`;

    const params = options?.ignoreNextRetryAt ? [batchSize] : [nowIso, batchSize];
    const dueRows = (await conn.execute(query, params)) as Array<{ id: string }>;

    let processed = 0;
    let rejected = 0;
    let stillPending = 0;

    for (const row of dueRows) {
      try {
        const res = await this.processWagerUseCase.reprocessPendingReference(
          row.id,
          maxAttempts,
          baseDelayMs,
        );

        if (!res) {
          continue;
        }
        if (res.status === WagerTransactionStatus.Processed) {
          processed += 1;
        } else if (res.status === WagerTransactionStatus.Rejected) {
          rejected += 1;
        } else {
          stillPending += 1;
        }
      } catch (err: unknown) {
        this.logger.warn('pending_reference.worker.reprocess_error', {
          transactionId: row.id,
          errorMessage: (err as Error)?.message,
        });
      }
    }

    return {
      checked: dueRows.length,
      processed,
      rejected,
      stillPending,
    };
  }

  private scheduleNextTick(): void {
    if (this.isShuttingDown) {
      return;
    }
    const intervalMs = Number(process.env.PENDING_REF_POLL_INTERVAL_MS ?? 200);
    this.timer = setTimeout(async () => {
      try {
        await this.sweepDuePendingReferences();
      } catch {
        // Ignore transient errors
      } finally {
        this.scheduleNextTick();
      }
    }, intervalMs);
  }
}
