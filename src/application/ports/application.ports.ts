import { SerializedIntegrationEvent } from '../../domain/shared/integration-event';

export const PROVIDER_IDENTITY_PORT = Symbol('PROVIDER_IDENTITY_PORT');

export interface AuthenticatedPrincipal {
  subject: string;
  providerId?: string;
  roles: string[];
  issuer: string;
  claims: Record<string, unknown>;
}

export abstract class ProviderIdentityPort {
  abstract verifyToken(bearerToken: string): Promise<AuthenticatedPrincipal>;
  abstract assertProviderAccess(
    principal: AuthenticatedPrincipal,
    providerId: string,
  ): void;
}

export const EVENT_PUBLISHER_PORT = Symbol('EVENT_PUBLISHER_PORT');

export abstract class EventPublisherPort {
  abstract publish(
    eventType: string,
    aggregateId: string,
    payload: SerializedIntegrationEvent<unknown> | Record<string, unknown>,
  ): Promise<void>;
  abstract isHealthy(): Promise<boolean>;
}

export const REALTIME_NOTIFIER_PORT = Symbol('REALTIME_NOTIFIER_PORT');

export abstract class RealtimeNotifierPort {
  abstract notifyOutboxEvent(
    eventType: string,
    payload: Record<string, unknown>,
  ): void;
  abstract notifyReconciliation(result: {
    walletId: string;
    storedBalance: { amount: string; currency: string };
    calculatedBalance: { amount: string; currency: string };
    difference: { amount: string; currency: string };
    consistent: boolean;
    checkedEntries: number;
  }): void;
}
