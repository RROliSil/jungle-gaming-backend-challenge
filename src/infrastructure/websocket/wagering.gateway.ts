import { Injectable } from '@nestjs/common';
import {
  ConnectedSocket,
  MessageBody,
  OnGatewayConnection,
  OnGatewayDisconnect,
  SubscribeMessage,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { RealtimeNotifierPort } from '../../application/ports/application.ports';
import { StructuredLogger } from '../observability/structured-logger';

export interface WsSubscriptionRequest {
  walletId?: string;
  playerId?: string;
  roundId?: string;
  providerId?: string;
  all?: boolean;
}

@Injectable()
@WebSocketGateway({
  namespace: '/ws/wagering',
  cors: {
    origin: '*',
  },
})
export class WageringWebsocketGateway
  implements RealtimeNotifierPort, OnGatewayConnection, OnGatewayDisconnect
{
  @WebSocketServer()
  public server?: Server;

  private readonly eventHistory: Array<{
    eventType: string;
    payload: Record<string, unknown>;
    emittedAt: string;
  }> = [];
  private readonly maxHistory = 200;

  constructor(private readonly logger: StructuredLogger) {}

  handleConnection(client: Socket): void {
    client.join('wagering:all');
    const walletId = client.handshake?.query?.walletId as string | undefined;
    const playerId = client.handshake?.query?.playerId as string | undefined;
    if (walletId) {
      client.join(`wallet:${walletId}`);
    }
    if (playerId) {
      client.join(`player:${playerId}`);
    }
    this.logger.info('websocket.client.connected', {
      clientId: client.id,
      walletId,
    });
  }

  handleDisconnect(client: Socket): void {
    this.logger.info('websocket.client.disconnected', {
      clientId: client.id,
    });
  }

  @SubscribeMessage('subscribe')
  handleSubscribe(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: WsSubscriptionRequest,
  ): { status: string; rooms: string[] } {
    const joined: string[] = [];
    if (body?.walletId) {
      const room = `wallet:${body.walletId}`;
      client.join(room);
      joined.push(room);
    }
    if (body?.playerId) {
      const room = `player:${body.playerId}`;
      client.join(room);
      joined.push(room);
    }
    if (body?.roundId) {
      const room = `round:${body.roundId}`;
      client.join(room);
      joined.push(room);
    }
    if (body?.providerId) {
      const room = `provider:${body.providerId}`;
      client.join(room);
      joined.push(room);
    }
    if (body?.all) {
      client.join('wagering:all');
      joined.push('wagering:all');
    }

    return {
      status: 'subscribed',
      rooms: joined,
    };
  }

  @SubscribeMessage('unsubscribe')
  handleUnsubscribe(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: WsSubscriptionRequest,
  ): { status: string; leftRooms: string[] } {
    const left: string[] = [];
    if (body?.walletId) {
      const room = `wallet:${body.walletId}`;
      client.leave(room);
      left.push(room);
    }
    if (body?.playerId) {
      const room = `player:${body.playerId}`;
      client.leave(room);
      left.push(room);
    }
    if (body?.roundId) {
      const room = `round:${body.roundId}`;
      client.leave(room);
      left.push(room);
    }
    if (body?.providerId) {
      const room = `provider:${body.providerId}`;
      client.leave(room);
      left.push(room);
    }

    return {
      status: 'unsubscribed',
      leftRooms: left,
    };
  }

  @SubscribeMessage('ping')
  handlePing(): { event: string; timestamp: string } {
    return {
      event: 'pong',
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Called strictly AFTER the SQL transaction and Outbox publish commit.
   */
  notifyOutboxEvent(
    eventType: string,
    payload: Record<string, unknown>,
  ): void {
    const emittedAt = new Date().toISOString();
    this.recordHistory(eventType, payload, emittedAt);

    if (!this.server) {
      return;
    }

    const data = (payload.data as Record<string, unknown>) ?? payload;
    const walletId =
      (data.walletId as string) ?? (payload.aggregateId as string);
    const playerId = data.playerId as string | undefined;
    const roundId = data.roundId as string | undefined;
    const providerId = data.providerId as string | undefined;

    this.server.to('wagering:all').emit(eventType, payload);

    if (walletId) {
      this.server.to(`wallet:${walletId}`).emit(eventType, payload);
    }
    if (playerId) {
      this.server.to(`player:${playerId}`).emit(eventType, payload);
    }
    if (roundId) {
      this.server.to(`round:${roundId}`).emit(eventType, payload);
    }
    if (providerId) {
      this.server.to(`provider:${providerId}`).emit(eventType, payload);
    }
  }

  notifyReconciliation(result: {
    walletId: string;
    storedBalance: { amount: string; currency: string };
    calculatedBalance: { amount: string; currency: string };
    difference: { amount: string; currency: string };
    consistent: boolean;
    checkedEntries: number;
  }): void {
    const eventType = 'WalletReconciliationCompleted';
    const payload: Record<string, unknown> = {
      eventType,
      occurredAt: new Date().toISOString(),
      data: result,
    };
    this.recordHistory(eventType, payload, new Date().toISOString());

    if (!this.server) {
      return;
    }

    this.server.to('wagering:all').emit(eventType, payload);
    this.server.to(`wallet:${result.walletId}`).emit(eventType, payload);
  }

  getRecentEmittedEvents(): ReadonlyArray<{
    eventType: string;
    payload: Record<string, unknown>;
    emittedAt: string;
  }> {
    return [...this.eventHistory];
  }

  clearHistory(): void {
    this.eventHistory.length = 0;
  }

  private recordHistory(
    eventType: string,
    payload: Record<string, unknown>,
    emittedAt: string,
  ): void {
    this.eventHistory.push({ eventType, payload, emittedAt });
    if (this.eventHistory.length > this.maxHistory) {
      this.eventHistory.shift();
    }
  }
}
