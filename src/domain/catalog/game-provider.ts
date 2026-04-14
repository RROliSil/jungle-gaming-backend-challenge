import { DomainError } from '../shared/domain-errors';

export enum ProviderStatus {
  Active = 'ACTIVE',
  Inactive = 'INACTIVE',
}

export interface CreateGameProviderProps {
  id: string;
  name: string;
  status?: ProviderStatus;
  webhookUrl?: string;
  allowedCurrencies?: string[];
  createdAt?: Date;
}

export interface GameProviderState {
  id: string;
  name: string;
  status: ProviderStatus;
  webhookUrl?: string;
  allowedCurrencies: string[];
  createdAt: Date;
  updatedAt: Date;
}

export class GameProvider {
  private constructor(
    public readonly id: string,
    private _name: string,
    private _status: ProviderStatus,
    private _webhookUrl: string | undefined,
    private _allowedCurrencies: string[],
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static create(props: CreateGameProviderProps): GameProvider {
    if (!props.id || props.id.trim().length === 0) {
      throw new DomainError('INVALID_PROVIDER_ID', 'Provider id is required');
    }
    if (!props.name || props.name.trim().length === 0) {
      throw new DomainError('INVALID_PROVIDER_NAME', 'Provider name is required');
    }
    const now = props.createdAt ? new Date(props.createdAt.getTime()) : new Date();
    const currencies = props.allowedCurrencies?.length
      ? [...new Set(props.allowedCurrencies.map((c) => c.toUpperCase()))]
      : ['BRL', 'USD', 'EUR'];

    return new GameProvider(
      props.id.trim(),
      props.name.trim(),
      props.status ?? ProviderStatus.Active,
      props.webhookUrl,
      currencies,
      now,
      now,
    );
  }

  static rehydrate(state: GameProviderState): GameProvider {
    return new GameProvider(
      state.id,
      state.name,
      state.status,
      state.webhookUrl,
      [...state.allowedCurrencies],
      new Date(state.createdAt.getTime()),
      new Date(state.updatedAt.getTime()),
    );
  }

  get name(): string {
    return this._name;
  }

  get status(): ProviderStatus {
    return this._status;
  }

  get webhookUrl(): string | undefined {
    return this._webhookUrl;
  }

  get allowedCurrencies(): ReadonlyArray<string> {
    return [...this._allowedCurrencies];
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  update(props: {
    name?: string;
    status?: ProviderStatus;
    webhookUrl?: string | null;
    allowedCurrencies?: string[];
    now?: Date;
  }): void {
    if (props.name !== undefined) {
      if (props.name.trim().length === 0) {
        throw new DomainError('INVALID_PROVIDER_NAME', 'Provider name cannot be empty');
      }
      this._name = props.name.trim();
    }
    if (props.status !== undefined) {
      this._status = props.status;
    }
    if (props.webhookUrl !== undefined) {
      this._webhookUrl = props.webhookUrl ?? undefined;
    }
    if (props.allowedCurrencies !== undefined && props.allowedCurrencies.length > 0) {
      this._allowedCurrencies = [
        ...new Set(props.allowedCurrencies.map((c) => c.toUpperCase())),
      ];
    }
    this._updatedAt = props.now ? new Date(props.now.getTime()) : new Date();
  }
}
