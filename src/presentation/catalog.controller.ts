import {
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  Patch,
  Post,
  Put,
  Query,
} from '@nestjs/common';
import {
  CatalogCrudUseCases,
  GameDto,
  ProviderDto,
} from '../application/use-cases/catalog-crud.use-cases';
import { ProviderStatus } from '../domain/catalog/game-provider';
import { MoneyProps } from '../domain/shared/money';

@Controller()
export class CatalogController {
  constructor(private readonly catalogUseCases: CatalogCrudUseCases) {}

  // ---- Providers CRUD ----

  @Post('providers')
  @HttpCode(HttpStatus.CREATED)
  async createProvider(
    @Body()
    body: {
      id: string;
      name: string;
      status?: ProviderStatus;
      webhookUrl?: string;
      allowedCurrencies?: string[];
    },
  ): Promise<ProviderDto> {
    return this.catalogUseCases.createProvider(body);
  }

  @Get('providers')
  async listProviders(): Promise<ProviderDto[]> {
    return this.catalogUseCases.listProviders();
  }

  @Get('providers/:providerId')
  async getProvider(@Param('providerId') providerId: string): Promise<ProviderDto> {
    return this.catalogUseCases.getProvider(providerId);
  }

  @Patch('providers/:providerId')
  async patchProvider(
    @Param('providerId') providerId: string,
    @Body()
    body: {
      name?: string;
      status?: ProviderStatus;
      webhookUrl?: string | null;
      allowedCurrencies?: string[];
    },
  ): Promise<ProviderDto> {
    return this.catalogUseCases.updateProvider(providerId, body);
  }

  @Put('providers/:providerId')
  async putProvider(
    @Param('providerId') providerId: string,
    @Body()
    body: {
      name?: string;
      status?: ProviderStatus;
      webhookUrl?: string | null;
      allowedCurrencies?: string[];
    },
  ): Promise<ProviderDto> {
    return this.catalogUseCases.updateProvider(providerId, body);
  }

  @Delete('providers/:providerId')
  async deleteProvider(
    @Param('providerId') providerId: string,
  ): Promise<{ deleted: boolean; id: string }> {
    return this.catalogUseCases.deleteProvider(providerId);
  }

  // ---- Games CRUD ----

  @Post('games')
  @HttpCode(HttpStatus.CREATED)
  async createGame(
    @Body()
    body: {
      id: string;
      providerId: string;
      name: string;
      category?: string;
      active?: boolean;
      minBet: MoneyProps;
      maxBet: MoneyProps;
    },
  ): Promise<GameDto> {
    return this.catalogUseCases.createGame(body);
  }

  @Get('games')
  async listGames(@Query('providerId') providerId?: string): Promise<GameDto[]> {
    return this.catalogUseCases.listGames(providerId);
  }

  @Get('games/:gameId')
  async getGame(@Param('gameId') gameId: string): Promise<GameDto> {
    return this.catalogUseCases.getGame(gameId);
  }

  @Patch('games/:gameId')
  async patchGame(
    @Param('gameId') gameId: string,
    @Body()
    body: {
      name?: string;
      category?: string;
      active?: boolean;
      minBet?: MoneyProps;
      maxBet?: MoneyProps;
    },
  ): Promise<GameDto> {
    return this.catalogUseCases.updateGame(gameId, body);
  }

  @Put('games/:gameId')
  async putGame(
    @Param('gameId') gameId: string,
    @Body()
    body: {
      name?: string;
      category?: string;
      active?: boolean;
      minBet?: MoneyProps;
      maxBet?: MoneyProps;
    },
  ): Promise<GameDto> {
    return this.catalogUseCases.updateGame(gameId, body);
  }

  @Delete('games/:gameId')
  async deleteGame(
    @Param('gameId') gameId: string,
  ): Promise<{ deleted: boolean; id: string }> {
    return this.catalogUseCases.deleteGame(gameId);
  }
}
