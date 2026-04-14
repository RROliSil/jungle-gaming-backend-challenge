import { createHash } from 'crypto';

/**
 * Deterministically serializes a JSON-compatible value by recursively sorting object keys
 * in lexicographical (UTF-16 code unit) order and omitting undefined properties.
 */
export function toCanonicalJson(value: unknown): string {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map((item) => toCanonicalJson(item)).join(',')}]`;
  }

  const record = value as Record<string, unknown>;
  const sortedKeys = Object.keys(record)
    .filter((key) => record[key] !== undefined)
    .sort();

  const entries = sortedKeys.map(
    (key) => `${JSON.stringify(key)}:${toCanonicalJson(record[key])}`,
  );

  return `{${entries.join(',')}}`;
}

/**
 * Computes a SHA-256 hex digest over the canonical JSON representation of a business payload.
 */
export function computeCanonicalPayloadHash(businessPayload: Record<string, unknown>): string {
  const canonical = toCanonicalJson(businessPayload);
  return createHash('sha256').update(canonical, 'utf8').digest('hex');
}

export interface WagerBusinessPayloadInput {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: {
    amount: string;
    currency: string;
  };
  referenceExternalTransactionId?: string;
}

/**
 * Extracts only the business fields of a wager request (excluding headers/transport metadata)
 * and computes its canonical SHA-256 payload hash.
 */
export function computeWagerPayloadHash(input: WagerBusinessPayloadInput): string {
  const subset: Record<string, unknown> = {
    externalTransactionId: input.externalTransactionId,
    gameId: input.gameId,
    kind: input.kind,
    money: {
      amount: input.money.amount,
      currency: input.money.currency,
    },
    playerId: input.playerId,
    providerId: input.providerId,
    roundId: input.roundId,
    walletId: input.walletId,
  };

  if (input.referenceExternalTransactionId !== undefined && input.referenceExternalTransactionId !== null) {
    subset.referenceExternalTransactionId = input.referenceExternalTransactionId;
  }

  return computeCanonicalPayloadHash(subset);
}
