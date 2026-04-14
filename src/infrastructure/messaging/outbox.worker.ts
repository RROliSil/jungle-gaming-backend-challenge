import { EntityManager } from '@mikro-orm/core';
import {
  Inject,
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
  Optional,
} from '@nestjs/common';
import {
  EVENT_PUBLISHER_PORT,
  EventPublisherPort,
  REALTIME_NOTIFIER_PORT,
  RealtimeNotifierPort,
} from '../../application/ports/application.ports';
import { OutboxMessage } from '../../domain/messaging/outbox-message';
import { MetricsService } from '../observability/metrics.service';
import { StructuredLogger } from '../observability/structured-logger';
import { OutboxMessageOrmEntity } from '../persistence/entities/orm-entities';
import { DomainPersistenceMapper } from '../persistence/mappers/domain-persistence.mapper';

export interface OutboxPublishBatchResult {
  claimed: number;
  published: number;
  retried: number;
  publishedIds: string[];
}

@Injectable()
export class OutboxWorker implements OnModuleInit, OnModuleDestroy {
  private timer?: ReturnType<typeof setTimeout>;
  private isShuttingDown = false;

  constructor(
    private readonly em: EntityManager,
    @Inject(EVENT_PUBLISHER_PORT)
    private readonly eventPublisher: EventPublisherPort,
    private readonly logger: StructuredLogger,
    private readonly metrics: MetricsService,
    @Optional()
    @Inject(REALTIME_NOTIFIER_PORT)
    private readonly realtimeNotifier?: RealtimeNotifierPort,
  ) {}

  onModuleInit(): void {
    if (process.env.OUTBOX_WORKER_DISABLED === 'true') {
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
   * Claims a batch of pending outbox messages using PostgreSQL `FOR UPDATE SKIP LOCKED`
   * so multiple concurrent publisher instances never block each other or double-claim
   * the same in-flight batch.
   */
  async processBatch(
    batchSize = Number(process.env.OUTBOX_BATCH_SIZE ?? 50),
  ): Promise<OutboxPublishBatchResult> {
    const publishedIds: string[] = [];
    let claimed = 0;
    let published = 0;
    let retried = 0;

    const committedEventsForWebsocket: Array<{
      eventType: string;
      payload: Record<string, unknown>;
    }> = [];

    await this.em.transactional(async (txEm) => {
      const now = new Date();
      const nowIso = now.toISOString();

      // Use raw SQL with FOR UPDATE SKIP LOCKED inside the active transaction context
      const conn = txEm.getConnection();
      const txCtx = txEm.getTransactionContext();
      const rawRows = (await conn.execute(
        `SELECT id, aggregate_id, event_type, payload, occurred_at, attempts, next_attempt_at, published_at
         FROM outbox_messages
         WHERE published_at IS NULL
           AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
         ORDER BY occurred_at ASC
         LIMIT ?
         FOR UPDATE SKIP LOCKED`,
        [nowIso, batchSize],
        'all',
        txCtx,
      )) as Array<{
        id: string;
        aggregate_id: string;
        event_type: string;
        payload: Record<string, unknown> | string;
        occurred_at: string | Date;
        attempts: number;
        next_attempt_at?: string | Date | null;
        published_at?: string | Date | null;
      }>;

      claimed = rawRows.length;

      for (const row of rawRows) {
        const ormEntity = new OutboxMessageOrmEntity();
        ormEntity.id = row.id;
        ormEntity.aggregateId = row.aggregate_id;
        ormEntity.eventType = row.event_type;
        ormEntity.payload =
          typeof row.payload === 'string'
            ? (JSON.parse(row.payload) as Record<string, unknown>)
            : row.payload;
        ormEntity.occurredAt = new Date(row.occurred_at);
        ormEntity.attempts = Number(row.attempts ?? 0);
        ormEntity.nextAttemptAt = row.next_attempt_at
          ? new Date(row.next_attempt_at)
          : null;
        ormEntity.publishedAt = row.published_at
          ? new Date(row.published_at)
          : null;

        const outboxMsg = DomainPersistenceMapper.toDomainOutboxMessage(ormEntity);

        try {
          await this.eventPublisher.publish(
            outboxMsg.eventType,
            outboxMsg.aggregateId,
            outboxMsg.payload,
          );

          outboxMsg.markPublished(now);
          published += 1;
          publishedIds.push(outboxMsg.id);

          await conn.execute(
            `UPDATE outbox_messages
             SET published_at = ?, attempts = ?
             WHERE id = ?`,
            [now.toISOString(), outboxMsg.attempts, outboxMsg.id],
            'run',
            txCtx,
          );

          committedEventsForWebsocket.push({
            eventType: outboxMsg.eventType,
            payload: { ...outboxMsg.payload },
          });
        } catch (err: unknown) {
          outboxMsg.scheduleRetry(now);
          retried += 1;
          this.metrics.incrementRetry('outbox_publisher');

          await conn.execute(
            `UPDATE outbox_messages
             SET attempts = ?, next_attempt_at = ?
             WHERE id = ?`,
            [
              outboxMsg.attempts,
              outboxMsg.nextAttemptAt?.toISOString() ?? null,
              outboxMsg.id,
            ],
            'run',
            txCtx,
          );

          this.logger.warn('outbox.publish.retry_scheduled', {
            eventId: outboxMsg.id,
            eventType: outboxMsg.eventType,
            aggregateId: outboxMsg.aggregateId,
            attempts: outboxMsg.attempts,
            errorMessage: (err as Error)?.message,
          });
        }
      }
    });

    // Notify WebSocket subscribers strictly AFTER the SQL transaction & Outbox commit
    if (this.realtimeNotifier) {
      for (const evt of committedEventsForWebsocket) {
        this.realtimeNotifier.notifyOutboxEvent(evt.eventType, evt.payload);
      }
    }

    await this.refreshOutboxLagMetrics();

    return {
      claimed,
      published,
      retried,
      publishedIds,
    };
  }

  async refreshOutboxLagMetrics(): Promise<void> {
    try {
      const forkEm = this.em.fork();
      const conn = forkEm.getConnection();
      const rows = (await conn.execute(
        `SELECT COUNT(*)::int AS pending_count,
                MIN(occurred_at) AS oldest_occurred_at
         FROM outbox_messages
         WHERE published_at IS NULL`,
      )) as Array<{ pending_count: number; oldest_occurred_at: string | Date | null }>;

      const row = rows[0];
      const pendingCount = Number(row?.pending_count ?? 0);
      let lagSeconds = 0;
      if (pendingCount > 0 && row?.oldest_occurred_at) {
        const oldestMs = new Date(row.oldest_occurred_at).getTime();
        lagSeconds = Math.max(0, (Date.now() - oldestMs) / 1000);
      }
      this.metrics.setOutboxMetrics(lagSeconds, pendingCount);
    } catch {
      // Ignore lag metrics failure during shutdown
    }
  }

  private scheduleNextTick(): void {
    if (this.isShuttingDown) {
      return;
    }
    const intervalMs = Number(process.env.OUTBOX_POLL_INTERVAL_MS ?? 200);
    this.timer = setTimeout(async () => {
      try {
        await this.processBatch();
      } catch {
        // Ignore transient worker errors
      } finally {
        this.scheduleNextTick();
      }
    }, intervalMs);
  }
}
