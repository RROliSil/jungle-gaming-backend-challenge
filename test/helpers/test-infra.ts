import { createHash, randomUUID } from 'crypto';
import { EventEmitter } from 'events';
import * as http from 'http';
import * as net from 'net';
import { Readable } from 'stream';
import { PassThrough } from 'stream';
import { PGlite } from '@electric-sql/pglite';
import { INestApplication } from '@nestjs/common';
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

class LocalSqsHttpBroker {
  public readonly port = 14566;
  private readonly queues = new Map<string, StoredSqsMessage[]>();

  constructor() {
    this.resetAllQueues();
  }

  start(): string {
    const baseUrl = `http://127.0.0.1:${this.port}`;
    process.env.SQS_ENDPOINT = baseUrl;
    process.env.SQS_WAGER_QUEUE_URL = `${baseUrl}/000000000000/wager-transactions.fifo`;
    process.env.SQS_WAGER_DLQ_URL = `${baseUrl}/000000000000/wager-transactions-dlq.fifo`;
    process.env.SQS_EVENTS_QUEUE_URL = `${baseUrl}/000000000000/wagering-events.fifo`;
    return baseUrl;
  }

  resetAllQueues(): void {
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
    const contentType = normalizedHeaders['content-type'] ?? '';

    if (
      target.startsWith('AmazonSQS.') ||
      contentType.includes('application/x-amz-json')
    ) {
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

    const params = new URLSearchParams(rawBody);
    const action = params.get('Action') ?? '';
    return {
      statusCode: 200,
      headers: { 'content-type': 'text/xml' },
      body: this.handleQueryAction(action, params),
    };
  }

  private handleJsonAction(
    action: string,
    payload: Record<string, unknown>,
  ): string {
    switch (action) {
      case 'CreateQueue': {
        const qName = String(payload.QueueName ?? 'wager-transactions.fifo');
        if (!this.queues.has(qName)) {
          this.queues.set(qName, []);
        }
        return JSON.stringify({
          QueueUrl: `http://127.0.0.1:${this.port}/000000000000/${qName}`,
        });
      }

      case 'GetQueueUrl': {
        const qName = String(payload.QueueName ?? 'wager-transactions.fifo');
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

  private handleQueryAction(action: string, params: URLSearchParams): string {
    const qUrl = params.get('QueueUrl') ?? '';
    const qName = this.extractQueueName(qUrl || (params.get('QueueName') ?? ''));

    switch (action) {
      case 'CreateQueue':
        if (!this.queues.has(qName)) {
          this.queues.set(qName, []);
        }
        return `<CreateQueueResponse><CreateQueueResult><QueueUrl>http://127.0.0.1:${this.port}/000000000000/${qName}</QueueUrl></CreateQueueResult></CreateQueueResponse>`;

      case 'GetQueueUrl':
        return `<GetQueueUrlResponse><GetQueueUrlResult><QueueUrl>http://127.0.0.1:${this.port}/000000000000/${qName}</QueueUrl></GetQueueUrlResult></GetQueueUrlResponse>`;

      case 'GetQueueAttributes': {
        const list = this.queues.get(qName) ?? [];
        return `<GetQueueAttributesResponse><GetQueueAttributesResult><Attribute><Name>ApproximateNumberOfMessages</Name><Value>${list.length}</Value></Attribute></GetQueueAttributesResult></GetQueueAttributesResponse>`;
      }

      case 'PurgeQueue':
        this.queues.set(qName, []);
        return `<PurgeQueueResponse></PurgeQueueResponse>`;

      case 'SendMessage': {
        if (!this.queues.has(qName)) {
          this.queues.set(qName, []);
        }
        const msgId = randomUUID();
        this.queues.get(qName)!.push({
          messageId: msgId,
          receiptHandle: randomUUID(),
          body: params.get('MessageBody') ?? '',
          messageGroupId: params.get('MessageGroupId') ?? 'default',
          deduplicationId: params.get('MessageDeduplicationId') ?? msgId,
          receiveCount: 0,
          visibleAt: Date.now(),
          sentTimestamp: Date.now(),
        });
        return `<SendMessageResponse><SendMessageResult><MessageId>${msgId}</MessageId></SendMessageResult></SendMessageResponse>`;
      }

      case 'ReceiveMessage': {
        const list = this.queues.get(qName) ?? [];
        const maxMessages = Number(params.get('MaxNumberOfMessages') ?? 10);
        const visibilityTimeoutSec = Number(params.get('VisibilityTimeout') ?? 15);
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

        const escapeXml = (s: string) =>
          s
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&apos;');

        const messagesXml = available
          .map(
            (m) =>
              `<Message><MessageId>${m.messageId}</MessageId><ReceiptHandle>${m.receiptHandle}</ReceiptHandle><Body>${escapeXml(m.body)}</Body><Attribute><Name>ApproximateReceiveCount</Name><Value>${m.receiveCount}</Value></Attribute></Message>`,
          )
          .join('');
        return `<ReceiveMessageResponse><ReceiveMessageResult>${messagesXml}</ReceiveMessageResult></ReceiveMessageResponse>`;
      }

      case 'DeleteMessage': {
        const receiptHandle = params.get('ReceiptHandle') ?? '';
        const list = this.queues.get(qName) ?? [];
        this.queues.set(
          qName,
          list.filter((m) => m.receiptHandle !== receiptHandle),
        );
        return `<DeleteMessageResponse></DeleteMessageResponse>`;
      }

      case 'ChangeMessageVisibility': {
        const receiptHandle = params.get('ReceiptHandle') ?? '';
        const visibilityTimeoutSec = Number(params.get('VisibilityTimeout') ?? 0);
        const list = this.queues.get(qName) ?? [];
        for (const item of list) {
          if (item.receiptHandle === receiptHandle) {
            item.visibleAt = Date.now() + visibilityTimeoutSec * 1000;
          }
        }
        return `<ChangeMessageVisibilityResponse></ChangeMessageVisibilityResponse>`;
      }

      default:
        return `<${action}Response></${action}Response>`;
    }
  }
}

async function isTcpPortOpen(port: number, host = '127.0.0.1'): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    const socket = new net.Socket();
    socket.setTimeout(300);
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

let pgliteInstance: PGlite | null = null;
let sqsBrokerInstance: LocalSqsHttpBroker | null = null;
let pgPatched = false;
let httpPatched = false;

const registeredNestServers = new Map<string, http.Server>();
let nextVirtualPort = 41000;

/**
 * Registers a NestJS application for in-process HTTP routing (supports both single and multi-instance tests)
 * so standard `fetch(baseUrl + '/path')` executes the full NestJS/Express pipeline even in network-restricted sandboxes.
 */
export async function startNestTestServer(app: INestApplication): Promise<string> {
  await app.init();
  const server = app.getHttpServer() as http.Server;
  const virtualPort = nextVirtualPort++;
  const baseUrl = `http://127.0.0.1:${virtualPort}`;
  registeredNestServers.set(`127.0.0.1:${virtualPort}`, server);
  registeredNestServers.set(`localhost:${virtualPort}`, server);
  return baseUrl;
}

/**
 * Dispatches an HTTP request directly into an Express/NestJS http.Server instance in memory.
 */
async function dispatchToNestServer(
  server: http.Server,
  urlObj: URL,
  init?: RequestInit,
): Promise<Response> {
  return new Promise<Response>((resolve, reject) => {
    const method = (init?.method ?? 'GET').toUpperCase();
    const rawHeaders: Record<string, string> = {};

    if (init?.headers) {
      if (init.headers instanceof Headers) {
        init.headers.forEach((v, k) => {
          rawHeaders[k.toLowerCase()] = v;
        });
      } else if (Array.isArray(init.headers)) {
        for (const [k, v] of init.headers) {
          rawHeaders[k.toLowerCase()] = v;
        }
      } else {
        for (const [k, v] of Object.entries(init.headers)) {
          if (v !== undefined) {
            rawHeaders[k.toLowerCase()] = String(v);
          }
        }
      }
    }

    const bodyBuffer =
      init?.body !== undefined && init?.body !== null
        ? Buffer.from(
            typeof init.body === 'string'
              ? init.body
              : JSON.stringify(init.body),
            'utf8',
          )
        : Buffer.alloc(0);

    if (bodyBuffer.length > 0 && !rawHeaders['content-length']) {
      rawHeaders['content-length'] = String(bodyBuffer.length);
    }

    const fakeSocket = new PassThrough() as unknown as net.Socket;
    (fakeSocket as unknown as Record<string, unknown>).remoteAddress = '127.0.0.1';
    (fakeSocket as unknown as Record<string, unknown>).writable = true;

    const req = new http.IncomingMessage(fakeSocket);
    req.method = method;
    req.url = `${urlObj.pathname}${urlObj.search}`;
    req.headers = rawHeaders;

    const resChunks: Buffer[] = [];
    const resHeaders: Record<string, string> = {};
    let statusCode = 200;

    const res = new http.ServerResponse(req);
    res.assignSocket(fakeSocket);

    const origSetHeader = res.setHeader.bind(res);
    res.setHeader = ((name: string, value: number | string | readonly string[]) => {
      resHeaders[name.toLowerCase()] = Array.isArray(value)
        ? value.join(', ')
        : String(value);
      return origSetHeader(name, value);
    }) as typeof res.setHeader;

    const origWriteHead = res.writeHead.bind(res);
    res.writeHead = ((
      code: number,
      reasonOrHeaders?: unknown,
      maybeHeaders?: unknown,
    ) => {
      statusCode = code;
      const hdrs =
        typeof reasonOrHeaders === 'object' && reasonOrHeaders !== null
          ? (reasonOrHeaders as Record<string, unknown>)
          : (maybeHeaders as Record<string, unknown> | undefined);
      if (hdrs) {
        for (const [k, v] of Object.entries(hdrs)) {
          if (v !== undefined) {
            resHeaders[k.toLowerCase()] = String(v);
          }
        }
      }
      return origWriteHead(code, reasonOrHeaders as string, maybeHeaders as http.OutgoingHttpHeaders);
    }) as typeof res.writeHead;

    res.write = ((chunk: unknown, encodingOrCb?: unknown, cb?: unknown) => {
      if (chunk) {
        resChunks.push(
          Buffer.isBuffer(chunk)
            ? chunk
            : Buffer.from(
                String(chunk),
                typeof encodingOrCb === 'string'
                  ? (encodingOrCb as BufferEncoding)
                  : 'utf8',
              ),
        );
      }
      if (typeof encodingOrCb === 'function') {
        encodingOrCb(null);
      } else if (typeof cb === 'function') {
        cb(null);
      }
      return true;
    }) as typeof res.write;

    res.end = ((chunk?: unknown, encodingOrCb?: unknown, cb?: unknown) => {
      if (chunk && typeof chunk !== 'function') {
        resChunks.push(
          Buffer.isBuffer(chunk)
            ? chunk
            : Buffer.from(
                String(chunk),
                typeof encodingOrCb === 'string'
                  ? (encodingOrCb as BufferEncoding)
                  : 'utf8',
              ),
        );
      }
      if (res.statusCode) {
        statusCode = res.statusCode;
      }
      if (typeof encodingOrCb === 'function') {
        encodingOrCb();
      } else if (typeof cb === 'function') {
        cb();
      }

      const fullBuffer = Buffer.concat(resChunks);
      resolve(
        new Response(fullBuffer, {
          status: statusCode,
          headers: resHeaders,
        }),
      );
      return res;
    }) as typeof res.end;

    try {
      server.emit('request', req, res);
      queueMicrotask(() => {
        if (bodyBuffer.length > 0) {
          req.emit('data', bodyBuffer);
        }
        req.emit('end');
      });
    } catch (err) {
      reject(err);
    }
  });
}

function patchHttpAndFetchForSandbox(sqsBroker: LocalSqsHttpBroker): void {
  if (httpPatched) {
    return;
  }
  httpPatched = true;

  const originalFetch = globalThis.fetch;
  globalThis.fetch = (async (
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> => {
    const urlStr =
      typeof input === 'string'
        ? input
        : input instanceof URL
          ? input.toString()
          : input.url;
    const urlObj = new URL(urlStr);
    const hostKey = `${urlObj.hostname}:${urlObj.port}`;

    const registeredServer = registeredNestServers.get(hostKey);
    if (registeredServer) {
      return dispatchToNestServer(registeredServer, urlObj, init);
    }

    if (Number(urlObj.port) === sqsBroker.port) {
      const reqHeaders: Record<string, string> = {};
      if (init?.headers) {
        for (const [k, v] of Object.entries(init.headers as Record<string, string>)) {
          reqHeaders[k] = String(v);
        }
      }
      const rawBody =
        typeof init?.body === 'string'
          ? init.body
          : init?.body
            ? Buffer.from(init.body as ArrayBuffer).toString('utf8')
            : '';
      const out = sqsBroker.handleDirectRequest(reqHeaders, rawBody);
      return new Response(out.body, {
        status: out.statusCode,
        headers: out.headers,
      });
    }

    return originalFetch(input, init);
  }) as typeof fetch;

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
      const out = sqsBroker.handleDirectRequest(request.headers ?? {}, rawBody);
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

class PGliteSessionCoordinator {
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

const sessionCoordinator = new PGliteSessionCoordinator();

function patchPgClientWithPGlite(pglite: PGlite): void {
  if (pgPatched) {
    return;
  }
  pgPatched = true;

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
      sessionCoordinator.release(this);
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
        await sessionCoordinator.acquire(clientRef);
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
          sessionCoordinator.release(clientRef);
        } else if (startedNewLock && !clientRef._inTx) {
          sessionCoordinator.release(clientRef);
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

export async function ensureTestInfrastructure(): Promise<{
  usingExternalPostgres: boolean;
  usingExternalSqs: boolean;
  resetSqsQueues: () => void;
}> {
  process.env.LOG_SILENT = process.env.LOG_SILENT ?? 'true';
  process.env.SQS_CONSUMER_DISABLED = 'true';
  process.env.OUTBOX_WORKER_DISABLED = 'true';
  process.env.PENDING_REF_WORKER_DISABLED = 'true';

  const pgPort = Number(process.env.DB_PORT ?? 5432);
  const pgOpen = await isTcpPortOpen(pgPort, process.env.DB_HOST ?? '127.0.0.1');

  if (!pgOpen) {
    if (!pgliteInstance) {
      pgliteInstance = await PGlite.create();
      patchPgClientWithPGlite(pgliteInstance);
    }
  }

  const sqsOpen = await isTcpPortOpen(4566, '127.0.0.1');
  if (!sqsOpen) {
    if (!sqsBrokerInstance) {
      sqsBrokerInstance = new LocalSqsHttpBroker();
      sqsBrokerInstance.start();
    }
    patchHttpAndFetchForSandbox(sqsBrokerInstance);
  }

  return {
    usingExternalPostgres: pgOpen,
    usingExternalSqs: sqsOpen,
    resetSqsQueues: () => {
      sqsBrokerInstance?.resetAllQueues();
    },
  };
}

export async function shutdownTestInfrastructure(): Promise<void> {
  registeredNestServers.clear();
}
