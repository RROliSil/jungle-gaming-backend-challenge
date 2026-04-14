import { Injectable } from '@nestjs/common';

export interface LogContext {
  correlationId?: string;
  messageId?: string;
  transactionId?: string;
  walletId?: string;
  providerId?: string;
  roundId?: string;
  kind?: string;
  status?: string;
  failureCode?: string;
  durationMs?: number;
  errorCode?: string;
  errorMessage?: string;
  [key: string]: unknown;
}

const REDACTED_KEYS = new Set([
  'password',
  'secret',
  'token',
  'authorization',
  'cookie',
  'money',
  'balance',
  'initialbalance',
  'balancebefore',
  'balanceafter',
  'payload',
  'rawpayload',
]);

function sanitizeContext(ctx: LogContext): Record<string, unknown> {
  const sanitized: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(ctx)) {
    if (value === undefined) {
      continue;
    }
    if (REDACTED_KEYS.has(key.toLowerCase())) {
      sanitized[key] = '[REDACTED]';
      continue;
    }
    sanitized[key] = value;
  }
  return sanitized;
}

@Injectable()
export class StructuredLogger {
  private readonly instanceId = process.env.INSTANCE_ID ?? `node-${process.pid}`;
  private readonly recentEntries: Record<string, unknown>[] = [];
  private readonly maxBuffer = 200;

  info(event: string, ctx: LogContext = {}): void {
    this.write('INFO', event, ctx);
  }

  warn(event: string, ctx: LogContext = {}): void {
    this.write('WARN', event, ctx);
  }

  error(event: string, ctx: LogContext = {}): void {
    this.write('ERROR', event, ctx);
  }

  getRecentLogs(): ReadonlyArray<Record<string, unknown>> {
    return [...this.recentEntries];
  }

  clearBuffer(): void {
    this.recentEntries.length = 0;
  }

  private write(level: 'INFO' | 'WARN' | 'ERROR', event: string, ctx: LogContext): void {
    const sanitized = sanitizeContext(ctx);
    const entry: Record<string, unknown> = {
      timestamp: new Date().toISOString(),
      level,
      service: 'wagering-processor',
      instanceId: this.instanceId,
      event,
      correlationId: (sanitized.correlationId as string) ?? null,
      messageId: (sanitized.messageId as string) ?? null,
      transactionId: (sanitized.transactionId as string) ?? null,
      walletId: (sanitized.walletId as string) ?? null,
      providerId: (sanitized.providerId as string) ?? null,
      ...sanitized,
    };

    this.recentEntries.push(entry);
    if (this.recentEntries.length > this.maxBuffer) {
      this.recentEntries.shift();
    }

    if (process.env.LOG_SILENT !== 'true') {
      const line = JSON.stringify(entry);
      if (level === 'ERROR') {
        process.stderr.write(`${line}\n`);
      } else {
        process.stdout.write(`${line}\n`);
      }
    }
  }
}
