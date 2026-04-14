import { DomainError } from '../shared/domain-errors';
import { Money } from '../shared/money';

export interface CreateGameProps {
  id: string;
  providerId: string;
  name: string;
  category?: string;
  active?: boolean;
  minBet: Money;
  maxBet: Money;
  createdAt?: Date;
}

export interface GameState {
  id: string;
  providerId: string;
  name: string;
  category: string;
  active: boolean;
  minBet: Money;
  maxBet: Money;
  createdAt: Date;
  updatedAt: Date;
}

export class Game {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    private _name: string,
    private _category: string,
    private _active: boolean,
    private _minBet: Money,
    private _maxBet: Money,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static create(props: CreateGameProps): Game {
    if (!props.id || props.id.trim().length === 0) {
      throw new DomainError('INVALID_GAME_ID', 'Game id is required');
    }
    if (!props.providerId || props.providerId.trim().length === 0) {
      throw new DomainError('INVALID_PROVIDER_ID', 'Game providerId is required');
    }
    if (!props.name || props.name.trim().length === 0) {
      throw new DomainError('INVALID_GAME_NAME', 'Game name is required');
    }
    if (props.maxBet.isLessThan(props.minBet)) {
      throw new DomainError('INVALID_BET_LIMITS', 'maxBet cannot be less than minBet');
    }

    const now = props.createdAt ? new Date(props.createdAt.getTime()) : new Date();
    return new Game(
      props.id.trim(),
      props.providerId.trim(),
      props.name.trim(),
      props.category?.trim() || 'SLOTS',
      props.active ?? true,
      props.minBet,
      props.maxBet,
      now,
      now,
    );
  }

  static rehydrate(state: GameState): Game {
    return new Game(
      state.id,
      state.providerId,
      state.name,
      state.category,
      state.active,
      state.minBet,
      state.maxBet,
      new Date(state.createdAt.getTime()),
      new Date(state.updatedAt.getTime()),
    );
  }

  get name(): string {
    return this._name;
  }

  get category(): string {
    return this._category;
  }

  get active(): boolean {
    return this._active;
  }

  get minBet(): Money {
    return this._minBet;
  }

  get maxBet(): Money {
    return this._maxBet;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  update(props: {
    name?: string;
    category?: string;
    active?: boolean;
    minBet?: Money;
    maxBet?: Money;
    now?: Date;
  }): void {
    if (props.name !== undefined) {
      if (props.name.trim().length === 0) {
        throw new DomainError('INVALID_GAME_NAME', 'Game name cannot be empty');
      }
      this._name = props.name.trim();
    }
    if (props.category !== undefined && props.category.trim().length > 0) {
      this._category = props.category.trim();
    }
    if (props.active !== undefined) {
      this._active = props.active;
    }
    const nextMin = props.minBet ?? this._minBet;
    const nextMax = props.maxBet ?? this._maxBet;
    if (nextMax.isLessThan(nextMin)) {
      throw new DomainError('INVALID_BET_LIMITS', 'maxBet cannot be less than minBet');
    }
    this._minBet = nextMin;
    this._maxBet = nextMax;
    this._updatedAt = props.now ? new Date(props.now.getTime()) : new Date();
  }
}
