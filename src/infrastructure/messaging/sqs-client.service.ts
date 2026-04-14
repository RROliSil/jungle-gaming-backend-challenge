import { randomUUID } from 'crypto';
import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  Message,
  PurgeQueueCommand,
  ReceiveMessageCommand,
  SQSClient,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { EventPublisherPort } from '../../application/ports/application.ports';
import { SerializedIntegrationEvent } from '../../domain/shared/integration-event';
import { StructuredLogger } from '../observability/structured-logger';

@Injectable()
export class SqsClientService implements EventPublisherPort, OnModuleDestroy {
  private client: SQSClient;

  constructor(private readonly logger: StructuredLogger) {
    this.client = this.createClient();
  }

  private createClient(): SQSClient {
    const sandboxHandler = (
      globalThis as unknown as { __SQS_SANDBOX_HANDLER__?: unknown }
    ).__SQS_SANDBOX_HANDLER__;

    return new SQSClient({
      region: process.env.AWS_REGION ?? 'us-east-1',
      endpoint: process.env.SQS_ENDPOINT ?? 'http://localhost:4566',
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
      },
      ...(sandboxHandler ? { requestHandler: sandboxHandler as never } : {}),
    });
  }

  refreshClient(): void {
    this.client.destroy();
    this.client = this.createClient();
  }

  get wagerQueueUrl(): string {
    return (
      process.env.SQS_WAGER_QUEUE_URL ??
      'http://localhost:4566/000000000000/wager-transactions.fifo'
    );
  }

  get wagerDlqUrl(): string {
    return (
      process.env.SQS_WAGER_DLQ_URL ??
      'http://localhost:4566/000000000000/wager-transactions-dlq.fifo'
    );
  }

  get eventsQueueUrl(): string {
    return (
      process.env.SQS_EVENTS_QUEUE_URL ??
      'http://localhost:4566/000000000000/wagering-events.fifo'
    );
  }

  async ensureQueuesExist(): Promise<void> {
    const fifoNames = [
      'wager-transactions-dlq.fifo',
      'wager-transactions.fifo',
      'wagering-events.fifo',
    ];

    for (const queueName of fifoNames) {
      try {
        await this.client.send(
          new CreateQueueCommand({
            QueueName: queueName,
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'false',
            },
          }),
        );
      } catch {
        // Ignore if queue already exists or endpoint is initializing
      }
    }
  }

  async publish(
    eventType: string,
    aggregateId: string,
    payload: SerializedIntegrationEvent<unknown> | Record<string, unknown>,
  ): Promise<void> {
    const eventId =
      (payload as { eventId?: string }).eventId ?? randomUUID();

    await this.client.send(
      new SendMessageCommand({
        QueueUrl: this.eventsQueueUrl,
        MessageBody: JSON.stringify(payload),
        MessageGroupId: aggregateId || 'default-group',
        MessageDeduplicationId: `${eventType}:${eventId}`,
      }),
    );
  }

  async sendWagerMessage(
    messageBody: Record<string, unknown>,
    messageGroupId: string,
    deduplicationId?: string,
  ): Promise<string> {
    const msgId = (messageBody.messageId as string) ?? randomUUID();
    const res = await this.client.send(
      new SendMessageCommand({
        QueueUrl: this.wagerQueueUrl,
        MessageBody: JSON.stringify(messageBody),
        MessageGroupId: messageGroupId,
        MessageDeduplicationId: deduplicationId ?? msgId,
      }),
    );
    return res.MessageId ?? msgId;
  }

  async sendToDlq(
    rawBody: string,
    reason: string,
    originalMessageId?: string,
  ): Promise<void> {
    const dlqEnvelope = {
      originalMessageId: originalMessageId ?? randomUUID(),
      reason,
      routedAt: new Date().toISOString(),
      rawBody,
    };

    await this.client.send(
      new SendMessageCommand({
        QueueUrl: this.wagerDlqUrl,
        MessageBody: JSON.stringify(dlqEnvelope),
        MessageGroupId: 'dlq-group',
        MessageDeduplicationId: randomUUID(),
      }),
    );
  }

  async receiveWagerMessages(maxMessages = 10, waitTimeSeconds = 0): Promise<Message[]> {
    const res = await this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: this.wagerQueueUrl,
        MaxNumberOfMessages: maxMessages,
        WaitTimeSeconds: waitTimeSeconds,
        VisibilityTimeout: 15,
        AttributeNames: ['All'],
        MessageSystemAttributeNames: ['All'],
      }),
    );
    return res.Messages ?? [];
  }

  async receiveDlqMessages(maxMessages = 10): Promise<Message[]> {
    const res = await this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: this.wagerDlqUrl,
        MaxNumberOfMessages: maxMessages,
        WaitTimeSeconds: 0,
        AttributeNames: ['All'],
      }),
    );
    return res.Messages ?? [];
  }

  async receivePublishedEvents(maxMessages = 10): Promise<Message[]> {
    const res = await this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: this.eventsQueueUrl,
        MaxNumberOfMessages: maxMessages,
        WaitTimeSeconds: 0,
        AttributeNames: ['All'],
      }),
    );
    return res.Messages ?? [];
  }

  async ackMessage(queueUrl: string, receiptHandle: string): Promise<void> {
    await this.client.send(
      new DeleteMessageCommand({
        QueueUrl: queueUrl,
        ReceiptHandle: receiptHandle,
      }),
    );
  }

  async changeVisibility(
    queueUrl: string,
    receiptHandle: string,
    visibilityTimeoutSeconds: number,
  ): Promise<void> {
    await this.client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: queueUrl,
        ReceiptHandle: receiptHandle,
        VisibilityTimeout: visibilityTimeoutSeconds,
      }),
    );
  }

  async purgeQueue(queueUrl: string): Promise<void> {
    try {
      await this.client.send(new PurgeQueueCommand({ QueueUrl: queueUrl }));
    } catch {
      // Fallback: drain messages manually if PurgeQueue is rate-limited
      const msgs = await this.client.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 0,
        }),
      );
      for (const m of msgs.Messages ?? []) {
        if (m.ReceiptHandle) {
          await this.ackMessage(queueUrl, m.ReceiptHandle);
        }
      }
    }
  }

  async isHealthy(): Promise<boolean> {
    try {
      await this.client.send(
        new GetQueueAttributesCommand({
          QueueUrl: this.wagerQueueUrl,
          AttributeNames: ['ApproximateNumberOfMessages'],
        }),
      );
      return true;
    } catch {
      try {
        await this.client.send(
          new GetQueueUrlCommand({ QueueName: 'wager-transactions.fifo' }),
        );
        return true;
      } catch (err) {
        this.logger.warn('sqs.health.check_failed', {
          errorMessage: (err as Error)?.message,
        });
        return false;
      }
    }
  }

  onModuleDestroy(): void {
    this.client.destroy();
  }
}
