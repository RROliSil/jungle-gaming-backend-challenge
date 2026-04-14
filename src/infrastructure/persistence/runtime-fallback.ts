import { createHash, randomUUID } from 'crypto';
import * as net from 'net';
import { Readable } from 'stream';
import { PGlite } from '@electric-sql/pglite';
import pg from 'pg';

interface StoredSqsMessage {
  messageId: string;
  receiptHandle: string;
  body: string;
  messageGroupId: string;
  deduplicationId: string;
  receiveCount: number;
  visibleAt: number;
  sentTimestamp: number;
}

class EmbeddedSqsBroker {
  public readonly port = 14566;
  private readonly queues = new Map<string, StoredSqsMessage[]>();

  constructor() {
    this.queues.set('wager-transactions.fifo', []);
    this.queues.set('wager-transactions-dlq.fifo', []);
    this.queues.set('wagering-events.fifo', []);
  }

  private extractQueueName(queueUrlOrName = ''): string {
    const parts = queueUrlOrName.split('/');
    return parts[parts.length - 1] || 'wager-transactions.fifo';
  }

  handleDirectRequest(
    headers: Record<string, string | string[] | undefined>,
    rawBody: string,
  ): { statusCode: number; headers: Record<string, string>; body: string } {
    const normalizedHeaders: Record<string, string> = {};
    for (const [k, v] of Object.entries(headers)) {
      if (typeof v === 'string') {
        normalizedHeaders[k.toLowerCase()] = v;
      } else if (Array.isArray(v)) {
        normalizedHeaders[k.toLowerCase()] = v.join(', ');
      }
    }

    const target = normalizedHeaders['x-amz-target'] ?? '';
    const action = target.replace('AmazonSQS.', '');
    const payload = rawBody
      ? (JSON.parse(rawBody) as Record<string, unknown>)
      : {};

    return {
      statusCode: 200,
      headers: { 'content-type': 'application/x-amz-json-1.0' },
      body: this.handleJsonAction(action, payload),
    };
  }

  private handleJsonAction(
    action: string,
    payload: Record<string, unknown>,
  ): string {
    switch (action) {
      case 'CreateQueue':
      case 'GetQueueUrl': {
        const qName = String(payload.QueueName ?? 'wager-transactions.fifo');
        if (!this.queues.has(qName)) {
          this.queues.set(qName, []);
        }
        return JSON.stringify({
          QueueUrl: `http://127.0.0.1:${this.port}/000000000000/${qName}`,
        });
      }

      case 'GetQueueAttributes': {
        const qName = this.extractQueueName(String(payload.QueueUrl ?? ''));
        const list = this.queues.get(qName) ?? [];
        return JSON.stringify({
          Attributes: {
            ApproximateNumberOfMessages: String(list.length),
          },
        });
      }

      case 'PurgeQueue': {
        const qName = this.extractQueueName(String(payload.QueueUrl ?? ''));
        this.queues.set(qName, []);
        return JSON.stringify({});
      }

      case 'SendMessage': {
        const qName = this.extractQueueName(String(payload.QueueUrl ?? ''));
        if (!this.queues.has(qName)) {
          this.queues.set(qName, []);
        }
        const list = this.queues.get(qName)!;
        const msgId = randomUUID();
        const dedupId = String(payload.MessageDeduplicationId ?? msgId);
        const bodyStr = String(payload.MessageBody ?? '');
        const md5 = createHash('md5').update(bodyStr, 'utf8').digest('hex');
        list.push({
          messageId: msgId,
          receiptHandle: randomUUID(),
          body: bodyStr,
          messageGroupId: String(payload.MessageGroupId ?? 'default'),
          deduplicationId: dedupId,
          receiveCount: 0,
          visibleAt: Date.now(),
          sentTimestamp: Date.now(),
        });
        return JSON.stringify({
          MessageId: msgId,
          MD5OfMessageBody: md5,
          SequenceNumber: String(Date.now()),
        });
      }

      case 'ReceiveMessage': {
        const qName = this.extractQueueName(String(payload.QueueUrl ?? ''));
        const list = this.queues.get(qName) ?? [];
        const maxMessages = Number(payload.MaxNumberOfMessages ?? 10);
        const visibilityTimeoutSec = Number(payload.VisibilityTimeout ?? 15);
        const now = Date.now();

        const available: StoredSqsMessage[] = [];
        for (const item of list) {
          if (item.visibleAt <= now && available.length < maxMessages) {
            item.receiveCount += 1;
            item.receiptHandle = randomUUID();
            item.visibleAt = now + visibilityTimeoutSec * 1000;
            available.push(item);
          }
        }

        return JSON.stringify({
          Messages: available.map((m) => ({
            MessageId: m.messageId,
            ReceiptHandle: m.receiptHandle,
            MD5OfBody: createHash('md5').update(m.body, 'utf8').digest('hex'),
            Body: m.body,
            Attributes: {
              ApproximateReceiveCount: String(m.receiveCount),
              SentTimestamp: String(m.sentTimestamp),
              MessageGroupId: m.messageGroupId,
              MessageDeduplicationId: m.deduplicationId,
            },
          })),
        });
      }

      case 'DeleteMessage': {
        const qName = this.extractQueueName(String(payload.QueueUrl ?? ''));
        const receiptHandle = String(payload.ReceiptHandle ?? '');
        const list = this.queues.get(qName) ?? [];
        this.queues.set(
          qName,
          list.filter((m) => m.receiptHandle !== receiptHandle),
        );
        return JSON.stringify({});
      }

      case 'ChangeMessageVisibility': {
        const qName = this.extractQueueName(String(payload.QueueUrl ?? ''));
        const receiptHandle = String(payload.ReceiptHandle ?? '');
        const visibilityTimeoutSec = Number(payload.VisibilityTimeout ?? 0);
        const list = this.queues.get(qName) ?? [];
        for (const item of list) {
          if (item.receiptHandle === receiptHandle) {
            item.visibleAt = Date.now() + visibilityTimeoutSec * 1000;
          }
        }
        return JSON.stringify({});
      }

      default:
        return JSON.stringify({});
    }
  }
}

class PGliteRuntimeCoordinator {
  private activeOwner: object | null = null;
  private readonly waitQueue: Array<() => void> = [];

  async acquire(owner: object): Promise<void> {
    if (this.activeOwner === owner) {
      return;
    }
    if (this.activeOwner === null) {
      this.activeOwner = owner;
      return;
    }
    await new Promise<void>((resolve) => {
      this.waitQueue.push(() => {
        this.activeOwner = owner;
        resolve();
      });
    });
  }

  release(owner: object): void {
    if (this.activeOwner !== owner) {
      return;
    }
    this.activeOwner = null;
    const next = this.waitQueue.shift();
    if (next) {
      next();
    }
  }
}

const runtimeCoordinator = new PGliteRuntimeCoordinator();
let runtimePglite: PGlite | null = null;
let runtimePatched = false;

async function isTcpPortOpen(port: number, host = '127.0.0.1'): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    const socket = new net.Socket();
    socket.setTimeout(350);
    socket.once('connect', () => {
      socket.destroy();
      resolve(true);
    });
    socket.once('timeout', () => {
      socket.destroy();
      resolve(false);
    });
    socket.once('error', () => {
      socket.destroy();
      resolve(false);
    });
    socket.connect(port, host);
  });
}

function patchPgForPglite(pglite: PGlite): void {
  if (runtimePatched) {
    return;
  }
  runtimePatched = true;

  const ClientProto = pg.Client.prototype as unknown as {
    connect: (cb?: (err: Error | null) => void) => Promise<void> | void;
    query: (
      configOrText: unknown,
      valuesOrCb?: unknown,
      cb?: unknown,
    ) => unknown;
    end: (cb?: (err?: Error) => void) => Promise<void> | void;
    _inTx?: boolean;
  };

  ClientProto.connect = function (
    this: { _inTx?: boolean },
    callback?: (err: Error | null) => void,
  ): Promise<void> | void {
    this._inTx = false;
    if (callback) {
      queueMicrotask(() => callback(null));
      return;
    }
    return Promise.resolve();
  };

  ClientProto.end = function (
    this: { _inTx?: boolean },
    callback?: (err?: Error) => void,
  ): Promise<void> | void {
    if (this._inTx) {
      this._inTx = false;
      runtimeCoordinator.release(this);
    }
    if (callback) {
      queueMicrotask(() => callback());
      return;
    }
    return Promise.resolve();
  };

  ClientProto.query = function (
    this: { _inTx?: boolean },
    configOrText: unknown,
    valuesOrCb?: unknown,
    cb?: unknown,
  ): unknown {
    let text = '';
    let values: unknown[] = [];
    let callback: ((err: Error | null, res?: unknown) => void) | undefined;

    if (typeof configOrText === 'string') {
      text = configOrText;
      if (typeof valuesOrCb === 'function') {
        callback = valuesOrCb as (err: Error | null, res?: unknown) => void;
      } else if (Array.isArray(valuesOrCb)) {
        values = valuesOrCb;
        if (typeof cb === 'function') {
          callback = cb as (err: Error | null, res?: unknown) => void;
        }
      }
    } else if (configOrText && typeof configOrText === 'object') {
      const qObj = configOrText as {
        text: string;
        values?: unknown[];
        callback?: (err: Error | null, res?: unknown) => void;
      };
      text = qObj.text;
      values = Array.isArray(qObj.values) ? qObj.values : [];
      callback =
        qObj.callback ??
        (typeof valuesOrCb === 'function'
          ? (valuesOrCb as (err: Error | null, res?: unknown) => void)
          : undefined);
    }

    const trimmedUpper = text.trim().toUpperCase();
    const isBegin =
      trimmedUpper === 'BEGIN' || trimmedUpper.startsWith('BEGIN;');
    const isCommitOrRollback =
      trimmedUpper === 'COMMIT' ||
      trimmedUpper.startsWith('COMMIT;') ||
      trimmedUpper === 'ROLLBACK' ||
      trimmedUpper.startsWith('ROLLBACK;');

    const runQuery = async () => {
      const clientRef = this;
      const startedNewLock = !clientRef._inTx;

      if (startedNewLock) {
        await runtimeCoordinator.acquire(clientRef);
      }

      if (isBegin) {
        clientRef._inTx = true;
      }

      try {
        let result: { rows: unknown[]; affectedRows?: number; fields?: unknown[] };
        const hasMultipleStatements =
          values.length === 0 &&
          text.includes(';') &&
          text.trim().slice(0, -1).includes(';');

        if (hasMultipleStatements) {
          const execResults = await pglite.exec(text);
          const last = execResults[execResults.length - 1] ?? {
            rows: [],
            affectedRows: 0,
            fields: [],
          };
          result = last;
        } else {
          result = await pglite.query(text, values);
        }

        return {
          rows: result.rows ?? [],
          rowCount: result.affectedRows ?? (result.rows ? result.rows.length : 0),
          command: trimmedUpper.split(/\s+/)[0] ?? 'SELECT',
          oid: 0,
          fields: result.fields ?? [],
        };
      } finally {
        if (isCommitOrRollback) {
          clientRef._inTx = false;
          runtimeCoordinator.release(clientRef);
        } else if (startedNewLock && !clientRef._inTx) {
          runtimeCoordinator.release(clientRef);
        }
      }
    };

    const promise = runQuery();
    if (callback) {
      promise.then(
        (res) => callback!(null, res),
        (err: Error) => callback!(err),
      );
      return configOrText;
    }
    return promise;
  };
}

export async function ensureRuntimeInfrastructure(): Promise<{
  postgresMode: 'docker-tcp' | 'embedded-pglite';
  sqsMode: 'docker-localstack' | 'embedded-sqs';
}> {
  const dbHost = process.env.DB_HOST ?? '127.0.0.1';
  const dbPort = Number(process.env.DB_PORT ?? 5432);
  const pgOpen = await isTcpPortOpen(dbPort, dbHost === 'localhost' ? '127.0.0.1' : dbHost);

  if (!pgOpen) {
    if (!runtimePglite) {
      runtimePglite = await PGlite.create();
      patchPgForPglite(runtimePglite);
    }
  }

  const sqsOpen = await isTcpPortOpen(4566, '127.0.0.1');
  if (!sqsOpen) {
    const broker = new EmbeddedSqsBroker();
    const baseUrl = `http://127.0.0.1:${broker.port}`;
    process.env.SQS_ENDPOINT = baseUrl;
    process.env.SQS_WAGER_QUEUE_URL = `${baseUrl}/000000000000/wager-transactions.fifo`;
    process.env.SQS_WAGER_DLQ_URL = `${baseUrl}/000000000000/wager-transactions-dlq.fifo`;
    process.env.SQS_EVENTS_QUEUE_URL = `${baseUrl}/000000000000/wagering-events.fifo`;

    (
      globalThis as unknown as {
        __SQS_SANDBOX_HANDLER__?: {
          handle: (request: {
            headers?: Record<string, string>;
            body?: unknown;
          }) => Promise<{
            response: {
              statusCode: number;
              headers: Record<string, string>;
              body: Readable;
            };
          }>;
        };
      }
    ).__SQS_SANDBOX_HANDLER__ = {
      handle: async (request) => {
        const rawBody =
          typeof request.body === 'string'
            ? request.body
            : request.body instanceof Uint8Array
              ? Buffer.from(request.body).toString('utf8')
              : '';
        const out = broker.handleDirectRequest(request.headers ?? {}, rawBody);
        return {
          response: {
            statusCode: out.statusCode,
            headers: out.headers,
            body: Readable.from([Buffer.from(out.body, 'utf8')]),
          },
        };
      },
    };
  }

  return {
    postgresMode: pgOpen ? 'docker-tcp' : 'embedded-pglite',
    sqsMode: sqsOpen ? 'docker-localstack' : 'embedded-sqs',
  };
}
