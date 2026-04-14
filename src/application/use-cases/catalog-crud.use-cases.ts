import { EntityManager } from '@mikro-orm/core';
import { Injectable } from '@nestjs/common';
import { Game } from '../../domain/catalog/game';
import { GameProvider, ProviderStatus } from '../../domain/catalog/game-provider';
import {
  DomainError,
  EntityNotFoundError,
} from '../../domain/shared/domain-errors';
import { Money, MoneyProps } from '../../domain/shared/money';
import {
  GameOrmEntity,
  GameProviderOrmEntity,
  WagerTransactionOrmEntity,
} from '../../infrastructure/persistence/entities/orm-entities';
import { DomainPersistenceMapper } from '../../infrastructure/persistence/mappers/domain-persistence.mapper';

export interface ProviderDto {
  id: string;
  name: string;
  status: ProviderStatus;
  webhookUrl?: string;
  allowedCurrencies: string[];
  createdAt: string;
  updatedAt: string;
}

export interface GameDto {
  id: string;
  providerId: string;
  name: string;
  category: string;
  active: boolean;
  minBet: MoneyProps;
  maxBet: MoneyProps;
  createdAt: string;
  updatedAt: string;
}

@Injectable()
export class CatalogCrudUseCases {
  constructor(private readonly em: EntityManager) {}

  // ---- Providers CRUD ----

  async createProvider(input: {
    id: string;
    name: string;
    status?: ProviderStatus;
    webhookUrl?: string;
    allowedCurrencies?: string[];
  }): Promise<ProviderDto> {
    const provider = GameProvider.create(input);

    await this.em.transactional(async (txEm) => {
      const existing = await txEm.findOne(GameProviderOrmEntity, { id: provider.id });
      if (existing) {
        throw new DomainError(
          'PROVIDER_ALREADY_EXISTS',
          `GameProvider '${provider.id}' already exists`,
        );
      }
      txEm.persist(DomainPersistenceMapper.toOrmGameProvider(provider));
      await txEm.flush();
    });

    return this.toProviderDto(provider);
  }

  async listProviders(): Promise<ProviderDto[]> {
    const rows = await this.em.find(
      GameProviderOrmEntity,
      {},
      { orderBy: { createdAt: 'ASC' } },
    );
    return rows.map((r) =>
      this.toProviderDto(DomainPersistenceMapper.toDomainGameProvider(r)),
    );
  }

  async getProvider(providerId: string): Promise<ProviderDto> {
    const row = await this.em.findOne(GameProviderOrmEntity, { id: providerId });
    if (!row) {
      throw new EntityNotFoundError('GameProvider', providerId);
    }
    return this.toProviderDto(DomainPersistenceMapper.toDomainGameProvider(row));
  }

  async updateProvider(
    providerId: string,
    input: {
      name?: string;
      status?: ProviderStatus;
      webhookUrl?: string | null;
      allowedCurrencies?: string[];
    },
  ): Promise<ProviderDto> {
    const updated = await this.em.transactional(async (txEm) => {
      const row = await txEm.findOne(GameProviderOrmEntity, { id: providerId });
      if (!row) {
        throw new EntityNotFoundError('GameProvider', providerId);
      }
      const domain = DomainPersistenceMapper.toDomainGameProvider(row);
      domain.update({ ...input, now: new Date() });
      DomainPersistenceMapper.toOrmGameProvider(domain, row);
      await txEm.flush();
      return domain;
    });

    return this.toProviderDto(updated);
  }

  async deleteProvider(providerId: string): Promise<{ deleted: boolean; id: string }> {
    await this.em.transactional(async (txEm) => {
      const row = await txEm.findOne(GameProviderOrmEntity, { id: providerId });
      if (!row) {
        throw new EntityNotFoundError('GameProvider', providerId);
      }

      const hasGames = await txEm.count(GameOrmEntity, { providerId });
      const hasTx = await txEm.count(WagerTransactionOrmEntity, { providerId });

      if (hasGames > 0 || hasTx > 0) {
        // Soft-deactivate if referenced by existing games or historical transactions
        const domain = DomainPersistenceMapper.toDomainGameProvider(row);
        domain.update({ status: ProviderStatus.Inactive, now: new Date() });
        DomainPersistenceMapper.toOrmGameProvider(domain, row);
      } else {
        txEm.remove(row);
      }
      await txEm.flush();
    });

    return { deleted: true, id: providerId };
  }

  // ---- Games CRUD ----

  async createGame(input: {
    id: string;
    providerId: string;
    name: string;
    category?: string;
    active?: boolean;
    minBet: MoneyProps;
    maxBet: MoneyProps;
  }): Promise<GameDto> {
    const minBet = Money.from(input.minBet);
    const maxBet = Money.from(input.maxBet);
    if (minBet.currency !== maxBet.currency) {
      throw new DomainError(
        'CURRENCY_MISMATCH',
        'Game minBet and maxBet must use the same currency',
      );
    }

    const game = Game.create({
      id: input.id,
      providerId: input.providerId,
      name: input.name,
      category: input.category,
      active: input.active,
      minBet,
      maxBet,
    });

    await this.em.transactional(async (txEm) => {
      const provider = await txEm.findOne(GameProviderOrmEntity, {
        id: game.providerId,
      });
      if (!provider) {
        throw new EntityNotFoundError('GameProvider', game.providerId);
      }

      const existing = await txEm.findOne(GameOrmEntity, { id: game.id });
      if (existing) {
        throw new DomainError(
          'GAME_ALREADY_EXISTS',
          `Game '${game.id}' already exists`,
        );
      }

      txEm.persist(DomainPersistenceMapper.toOrmGame(game));
      await txEm.flush();
    });

    return this.toGameDto(game);
  }

  async listGames(providerId?: string): Promise<GameDto[]> {
    const where = providerId ? { providerId } : {};
    const rows = await this.em.find(GameOrmEntity, where, {
      orderBy: { createdAt: 'ASC' },
    });
    return rows.map((r) =>
      this.toGameDto(DomainPersistenceMapper.toDomainGame(r)),
    );
  }

  async getGame(gameId: string): Promise<GameDto> {
    const row = await this.em.findOne(GameOrmEntity, { id: gameId });
    if (!row) {
      throw new EntityNotFoundError('Game', gameId);
    }
    return this.toGameDto(DomainPersistenceMapper.toDomainGame(row));
  }

  async updateGame(
    gameId: string,
    input: {
      name?: string;
      category?: string;
      active?: boolean;
      minBet?: MoneyProps;
      maxBet?: MoneyProps;
    },
  ): Promise<GameDto> {
    const updated = await this.em.transactional(async (txEm) => {
      const row = await txEm.findOne(GameOrmEntity, { id: gameId });
      if (!row) {
        throw new EntityNotFoundError('Game', gameId);
      }

      const domain = DomainPersistenceMapper.toDomainGame(row);
      domain.update({
        name: input.name,
        category: input.category,
        active: input.active,
        minBet: input.minBet ? Money.from(input.minBet) : undefined,
        maxBet: input.maxBet ? Money.from(input.maxBet) : undefined,
        now: new Date(),
      });
      DomainPersistenceMapper.toOrmGame(domain, row);
      await txEm.flush();
      return domain;
    });

    return this.toGameDto(updated);
  }

  async deleteGame(gameId: string): Promise<{ deleted: boolean; id: string }> {
    await this.em.transactional(async (txEm) => {
      const row = await txEm.findOne(GameOrmEntity, { id: gameId });
      if (!row) {
        throw new EntityNotFoundError('Game', gameId);
      }

      const hasTx = await txEm.count(WagerTransactionOrmEntity, { gameId });
      if (hasTx > 0) {
        const domain = DomainPersistenceMapper.toDomainGame(row);
        domain.update({ active: false, now: new Date() });
        DomainPersistenceMapper.toOrmGame(domain, row);
      } else {
        txEm.remove(row);
      }
      await txEm.flush();
    });

    return { deleted: true, id: gameId };
  }

  private toProviderDto(provider: GameProvider): ProviderDto {
    return {
      id: provider.id,
      name: provider.name,
      status: provider.status,
      webhookUrl: provider.webhookUrl,
      allowedCurrencies: [...provider.allowedCurrencies],
      createdAt: provider.createdAt.toISOString(),
      updatedAt: provider.updatedAt.toISOString(),
    };
  }

  private toGameDto(game: Game): GameDto {
    return {
      id: game.id,
      providerId: game.providerId,
      name: game.name,
      category: game.category,
      active: game.active,
      minBet: game.minBet.toJSON(),
      maxBet: game.maxBet.toJSON(),
      createdAt: game.createdAt.toISOString(),
      updatedAt: game.updatedAt.toISOString(),
    };
  }
}
