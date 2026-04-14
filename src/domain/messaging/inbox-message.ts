import { DomainError } from '../shared/domain-errors';

export interface ReceiveInboxProps {
  messageId: string;
  consumerName: string;
  payloadHash: string;
  receivedAt?: Date;
}

export interface InboxMessageState {
  messageId: string;
  consumerName: string;
  payloadHash: string;
  receivedAt: Date;
  processedAt?: Date;
}

export class InboxMessage {
  private constructor(
    public readonly messageId: string,
    public readonly consumerName: string,
    public readonly payloadHash: string,
    public readonly receivedAt: Date,
    private _processedAt?: Date,
  ) {}

  static receive(props: ReceiveInboxProps): InboxMessage {
    if (!props.messageId || props.messageId.trim().length === 0) {
      throw new DomainError('INVALID_INBOX_MESSAGE_ID', 'messageId is required');
    }
    if (!props.consumerName || props.consumerName.trim().length === 0) {
      throw new DomainError('INVALID_INBOX_CONSUMER_NAME', 'consumerName is required');
    }
    if (!props.payloadHash || props.payloadHash.trim().length === 0) {
      throw new DomainError('INVALID_INBOX_PAYLOAD_HASH', 'payloadHash is required');
    }

    const now = props.receivedAt ? new Date(props.receivedAt.getTime()) : new Date();
    return new InboxMessage(
      props.messageId,
      props.consumerName,
      props.payloadHash,
      now,
      undefined,
    );
  }

  static rehydrate(state: InboxMessageState): InboxMessage {
    return new InboxMessage(
      state.messageId,
      state.consumerName,
      state.payloadHash,
      new Date(state.receivedAt.getTime()),
      state.processedAt ? new Date(state.processedAt.getTime()) : undefined,
    );
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  isProcessed(): boolean {
    return this._processedAt !== undefined;
  }

  markProcessed(at: Date): void {
    if (this._processedAt) {
      return;
    }
    this._processedAt = new Date(at.getTime());
  }
}
