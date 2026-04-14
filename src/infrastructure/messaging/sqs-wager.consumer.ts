import { createHash, randomUUID } from 'crypto';
import { Message } from '@aws-sdk/client-sqs';
import {
  BeforeApplicationShutdown,
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ProcessWagerTransactionUseCase } from '../../application/use-cases/process-wager-transaction.use-case';
import {
  DomainError,
  EntityNotFoundError,
  IdempotencyConflictError,
  InvalidMoneyError,
  InvalidTransactionPropsError,
} from '../../domain/shared/domain-errors';
import { WagerTransactionKind } from '../../domain/wagering/wager-transaction';
import { MetricsService } from '../observability/metrics.service';
import { StructuredLogger } from '../observability/structured-logger';
import { SqsClientService } from './sqs-client.service';

export interface SqsWagerEnvelope {
  messageId: string;
  type: 'WagerTransactionRequested';
  occurredAt: string;
  correlationId?: string;
  data: {
    providerId: string;
    externalTransactionId: string;
    idempotencyKey?: string;
    playerId: string;
    walletId: string;
    roundId: string;
    gameId: string;
    kind: WagerTransactionKind;
    money: {
      amount: string;
      currency: string;
    };
    referenceExternalTransactionId?: string;
  };
}

export interface ProcessSqsMessageOptions {
  /** Simulates worker crash after SQL commit and before SQS DeleteMessage ack (Section 13 Scenario 5) */
  simulateCrashAfterCommitBeforeAck?: boolean;
  /** Forces a transient error before commit to test retry/DLQ behavior */
  simulateTransientFailure?: boolean;
}

@Injectable()
export class SqsWagerConsumer
  implements OnModuleInit, OnModuleDestroy, BeforeApplicationShutdown
{
  public static readonly CONSUMER_NAME = 'wager-transactions-sqs-consumer';

  private pollingTimer?: ReturnType<typeof setTimeout>;
  private isShuttingDown = false;
  private readonly inFlightMessages = new Map<string, string>(); // messageId -> receiptHandle

  constructor(
    private readonly sqsClient: SqsClientService,
    private readonly processWagerUseCase: ProcessWagerTransactionUseCase,
    private readonly logger: StructuredLogger,
    private readonly metrics: MetricsService,
  ) {}

  onModuleInit(): void {
    if (process.env.SQS_CONSUMER_DISABLED === 'true') {
      return;
    }
    this.scheduleNextPoll();
  }

  async beforeApplicationShutdown(signal?: string): Promise<void> {
    this.logger.info('sqs.consumer.shutdown_initiated', { signal: signal ?? 'SIGTERM' });
    await this.gracefulStop();
  }

  async onModuleDestroy(): Promise<void> {
    await this.gracefulStop();
  }

  async gracefulStop(timeoutMs = 3000): Promise<void> {
    this.isShuttingDown = true;
    if (this.pollingTimer) {
      clearTimeout(this.pollingTimer);
      this.pollingTimer = undefined;
    }

    const deadline = Date.now() + timeoutMs;
    while (this.inFlightMessages.size > 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 25));
    }

    // Section 10: em SIGTERM, concluir mensagens em andamento ou devolver a visibilidade
    if (this.inFlightMessages.size > 0) {
      for (const [msgId, receiptHandle] of this.inFlightMessages.entries()) {
        try {
          await this.sqsClient.changeVisibility(
            this.sqsClient.wagerQueueUrl,
            receiptHandle,
            0,
          );
          this.logger.warn('sqs.consumer.visibility_returned_on_shutdown', {
            messageId: msgId,
          });
        } catch {
          // Ignore errors during shutdown visibility release
        }
      }
      this.inFlightMessages.clear();
    }
  }

  /**
   * Polls one batch of messages from wager-transactions.fifo and processes them.
   */
  async pollOnce(options?: ProcessSqsMessageOptions): Promise<number> {
    if (this.isShuttingDown && !options) {
      return 0;
    }

    const messages = await this.sqsClient.receiveWagerMessages(10, 0);
    for (const msg of messages) {
      if (this.isShuttingDown && !options) {
        if (msg.ReceiptHandle) {
          await this.sqsClient.changeVisibility(
            this.sqsClient.wagerQueueUrl,
            msg.ReceiptHandle,
            0,
          );
        }
        break;
      }
      await this.handleSqsMessage(msg, options);
    }

    return messages.length;
  }

  /**
   * Processes a single SQS Message according to Section 10 rules:
   * - Reuses the same use case as HTTP;
   * - Deduplicates via persistent inbox by (consumerName, messageId);
   * - Acks ONLY after SQL commit;
   * - Distinguishes business (terminal -> ack), transient (retry with backoff), and permanent (DLQ) errors;
   * - Respects retry limit before DLQ.
   */
  async handleSqsMessage(
    msg: Message,
    options?: ProcessSqsMessageOptions,
  ): Promise<void> {
    const receiptHandle = msg.ReceiptHandle;
    const rawBody = msg.Body ?? '';
    const receiveCount = Number(msg.Attributes?.ApproximateReceiveCount ?? '1');
    const maxReceiveCount = Number(process.env.SQS_MAX_RECEIVE_COUNT ?? '3');

    let envelope: SqsWagerEnvelope;
    try {
      envelope = JSON.parse(rawBody) as SqsWagerEnvelope;
      if (
        !envelope ||
        typeof envelope !== 'object' ||
        !envelope.messageId ||
        envelope.type !== 'WagerTransactionRequested' ||
        !envelope.data
      ) {
        throw new Error('Malformed WagerTransactionRequested envelope');
      }
    } catch (parseErr) {
      // Permanent error: malformed message cannot succeed on retry -> route to DLQ & ack
      this.metrics.incrementDlqMessages();
      this.logger.error('sqs.consumer.permanent_malformed_message_dlq', {
        messageId: msg.MessageId,
        errorMessage: (parseErr as Error)?.message,
      });
      await this.sqsClient.sendToDlq(
        rawBody,
        `MALFORMED_ENVELOPE: ${(parseErr as Error)?.message ?? 'invalid JSON'}`,
        msg.MessageId,
      );
      if (receiptHandle) {
        await this.sqsClient.ackMessage(this.sqsClient.wagerQueueUrl, receiptHandle);
      }
      return;
    }

    const messageId = envelope.messageId;
    const correlationId = envelope.correlationId ?? randomUUID();

    if (receiptHandle) {
      this.inFlightMessages.set(messageId, receiptHandle);
    }

    try {
      if (options?.simulateTransientFailure) {
        throw new Error('SIMULATED_TRANSIENT_INFRASTRUCTURE_ERROR');
      }

      const rawPayloadHash = createHash('sha256')
        .update(rawBody, 'utf8')
        .digest('hex');

      const data = envelope.data;
      const idempotencyKey =
        data.idempotencyKey ?? `${data.providerId}:${data.externalTransactionId}`;

      await this.processWagerUseCase.execute({
        idempotencyKey,
        providerId: data.providerId,
        externalTransactionId: data.externalTransactionId,
        playerId: data.playerId,
        walletId: data.walletId,
        roundId: data.roundId,
        gameId: data.gameId,
        kind: data.kind,
        money: data.money,
        referenceExternalTransactionId: data.referenceExternalTransactionId,
        correlationId,
        inboxContext: {
          consumerName: SqsWagerConsumer.CONSUMER_NAME,
          messageId,
          payloadHash: rawPayloadHash,
        },
      });

      // Simulate worker dying AFTER SQL commit and BEFORE SQS ack (Section 13 Concurrency Scenario 5)
      if (options?.simulateCrashAfterCommitBeforeAck) {
        if (receiptHandle) {
          await this.sqsClient.changeVisibility(
            this.sqsClient.wagerQueueUrl,
            receiptHandle,
            0,
          );
        }
        throw new Error('WORKER_CRASHED_AFTER_COMMIT_BEFORE_ACK');
      }

      // Section 10: fazer ack somente após o commit
      if (receiptHandle) {
        await this.sqsClient.ackMessage(this.sqsClient.wagerQueueUrl, receiptHandle);
      }
    } catch (err: unknown) {
      if ((err as Error)?.message === 'WORKER_CRASHED_AFTER_COMMIT_BEFORE_ACK') {
        throw err;
      }

      // Distinguish Business / Permanent Contract errors from Transient errors
      if (this.isPermanentDomainOrContractError(err)) {
        // If it's an unrecoverable contract error (e.g. invalid Money format, missing required field,
        // unknown wallet, or idempotency conflict), route to DLQ and ack so it doesn't poison the queue
        this.metrics.incrementDlqMessages();
        this.logger.warn('sqs.consumer.permanent_contract_error_dlq', {
          correlationId,
          messageId,
          walletId: envelope.data?.walletId,
          providerId: envelope.data?.providerId,
          errorCode: (err as DomainError)?.code,
          errorMessage: (err as Error)?.message,
        });
        await this.sqsClient.sendToDlq(
          rawBody,
          `PERMANENT_DOMAIN_ERROR:${(err as DomainError)?.code ?? 'INVALID_CONTRACT'}:${(err as Error)?.message}`,
          messageId,
        );
        if (receiptHandle) {
          await this.sqsClient.ackMessage(this.sqsClient.wagerQueueUrl, receiptHandle);
        }
        return;
      }

      // Transient error: check retry budget before DLQ
      if (receiveCount >= maxReceiveCount) {
        this.metrics.incrementDlqMessages();
        this.logger.error('sqs.consumer.max_retries_exceeded_dlq', {
          correlationId,
          messageId,
          walletId: envelope.data?.walletId,
          providerId: envelope.data?.providerId,
          receiveCount,
          errorMessage: (err as Error)?.message,
        });
        await this.sqsClient.sendToDlq(
          rawBody,
          `MAX_RETRIES_EXCEEDED(${receiveCount}): ${(err as Error)?.message ?? 'transient failure'}`,
          messageId,
        );
        if (receiptHandle) {
          await this.sqsClient.ackMessage(this.sqsClient.wagerQueueUrl, receiptHandle);
        }
        return;
      }

      // Retry with exponential visibility backoff
      this.metrics.incrementRetry('sqs_consumer');
      const backoffSeconds = Math.min(30, Math.pow(2, receiveCount));
      this.logger.warn('sqs.consumer.transient_error_retry_scheduled', {
        correlationId,
        messageId,
        receiveCount,
        backoffSeconds,
        errorMessage: (err as Error)?.message,
      });

      if (receiptHandle) {
        try {
          await this.sqsClient.changeVisibility(
            this.sqsClient.wagerQueueUrl,
            receiptHandle,
            backoffSeconds,
          );
        } catch {
          // Visibility timeout will expire naturally if ChangeMessageVisibility fails
        }
      }
    } finally {
      this.inFlightMessages.delete(messageId);
    }
  }

  private isPermanentDomainOrContractError(err: unknown): boolean {
    return (
      err instanceof InvalidMoneyError ||
      err instanceof InvalidTransactionPropsError ||
      err instanceof IdempotencyConflictError ||
      err instanceof EntityNotFoundError
    );
  }

  private scheduleNextPoll(): void {
    if (this.isShuttingDown) {
      return;
    }
    const intervalMs = Number(process.env.SQS_POLL_INTERVAL_MS ?? 200);
    this.pollingTimer = setTimeout(async () => {
      try {
        await this.pollOnce();
      } catch {
        // Ignore background poll error when LocalStack is not running
      } finally {
        this.scheduleNextPoll();
      }
    }, intervalMs);
  }
}
