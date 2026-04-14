import { IntegrationEvent } from '../shared/integration-event';

export interface OutboxMessageState {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: Readonly<Record<string, unknown>>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt?: Date;
  publishedAt?: Date;
}

export class OutboxMessage {
  private constructor(
    public readonly id: string,
    public readonly aggregateId: string,
    public readonly eventType: string,
    public readonly payload: Readonly<Record<string, unknown>>,
    public readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt?: Date,
    private _publishedAt?: Date,
  ) {}

  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    const serialized = event.toJSON() as unknown as Record<string, unknown>;
    return new OutboxMessage(
      event.eventId,
      event.aggregateId,
      event.eventType,
      Object.freeze({ ...serialized }),
      new Date(event.occurredAt.getTime()),
      0,
      new Date(event.occurredAt.getTime()),
      undefined,
    );
  }

  static rehydrate(state: OutboxMessageState): OutboxMessage {
    return new OutboxMessage(
      state.id,
      state.aggregateId,
      state.eventType,
      Object.freeze({ ...state.payload }),
      new Date(state.occurredAt.getTime()),
      state.attempts,
      state.nextAttemptAt ? new Date(state.nextAttemptAt.getTime()) : undefined,
      state.publishedAt ? new Date(state.publishedAt.getTime()) : undefined,
    );
  }

  get attempts(): number {
    return this._attempts;
  }

  get nextAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }

  get publishedAt(): Date | undefined {
    return this._publishedAt;
  }

  isPending(): boolean {
    return this._publishedAt === undefined;
  }

  isDue(now: Date): boolean {
    if (!this.isPending()) {
      return false;
    }
    if (!this._nextAttemptAt) {
      return true;
    }
    return this._nextAttemptAt.getTime() <= now.getTime();
  }

  markPublished(at: Date): void {
    this._publishedAt = new Date(at.getTime());
  }

  /** incrementa attempts e calcula o próximo nextAttemptAt (backoff exponencial) */
  scheduleRetry(now: Date, baseDelayMs = 250): void {
    this._attempts += 1;
    const backoffMs = Math.min(
      30_000,
      baseDelayMs * Math.pow(2, Math.max(0, this._attempts - 1)),
    );
    this._nextAttemptAt = new Date(now.getTime() + backoffMs);
  }
}
