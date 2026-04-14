import {
  Body,
  Controller,
  Delete,
  Get,
  Headers,
  HttpCode,
  HttpStatus,
  Param,
  Patch,
  Post,
  Query,
} from '@nestjs/common';
import {
  LedgerPageResponseDto,
  ReconciliationResponseDto,
  WalletResponseDto,
  WalletUseCases,
} from '../application/use-cases/wallet.use-cases';
import { InvalidTransactionPropsError } from '../domain/shared/domain-errors';
import { MoneyProps } from '../domain/shared/money';
import { WalletStatus } from '../domain/wallet/wallet';

@Controller('wallets')
export class WalletsController {
  constructor(private readonly walletUseCases: WalletUseCases) {}

  @Post()
  @HttpCode(HttpStatus.CREATED)
  async createWallet(
    @Body()
    body: {
      id?: string;
      playerId: string;
      initialBalance: MoneyProps;
      metadata?: Record<string, unknown>;
    },
    @Headers('x-correlation-id') correlationId?: string,
  ): Promise<WalletResponseDto> {
    if (!body || typeof body !== 'object') {
      throw new InvalidTransactionPropsError('INVALID_PAYLOAD', 'Request body is required');
    }
    if (!body.playerId || typeof body.playerId !== 'string') {
      throw new InvalidTransactionPropsError('INVALID_PLAYER_ID', 'playerId is required');
    }
    if (!body.initialBalance || typeof body.initialBalance !== 'object') {
      throw new InvalidTransactionPropsError(
        'INVALID_INITIAL_BALANCE',
        'initialBalance object { amount, currency } is required',
      );
    }

    return this.walletUseCases.createWallet({
      id: body.id,
      playerId: body.playerId,
      initialBalance: body.initialBalance,
      metadata: body.metadata,
      correlationId,
    });
  }

  @Get()
  async listWallets(
    @Query('playerId') playerId?: string,
    @Query('currency') currency?: string,
    @Query('status') status?: string,
    @Query('limit') limit?: string,
  ): Promise<WalletResponseDto[]> {
    return this.walletUseCases.listWallets({
      playerId,
      currency,
      status,
      limit: limit ? Number(limit) : undefined,
    });
  }

  @Get(':walletId')
  async getWallet(@Param('walletId') walletId: string): Promise<WalletResponseDto> {
    return this.walletUseCases.getWallet(walletId);
  }

  @Patch(':walletId')
  async updateWallet(
    @Param('walletId') walletId: string,
    @Body()
    body: {
      status?: WalletStatus;
      metadata?: Record<string, unknown>;
    },
  ): Promise<WalletResponseDto> {
    return this.walletUseCases.updateWallet(walletId, {
      status: body?.status,
      metadata: body?.metadata,
    });
  }

  @Delete(':walletId')
  async closeWallet(@Param('walletId') walletId: string): Promise<WalletResponseDto> {
    return this.walletUseCases.closeWallet(walletId);
  }

  @Get(':walletId/ledger')
  async getWalletLedger(
    @Param('walletId') walletId: string,
    @Query('cursor') cursor?: string,
    @Query('limit') limit?: string,
  ): Promise<LedgerPageResponseDto> {
    return this.walletUseCases.listWalletLedger({
      walletId,
      cursor,
      limit: limit ? Number(limit) : 50,
    });
  }

  @Post(':walletId/reconciliation')
  @HttpCode(HttpStatus.OK)
  async reconcileWallet(
    @Param('walletId') walletId: string,
    @Headers('x-correlation-id') correlationId?: string,
  ): Promise<ReconciliationResponseDto> {
    return this.walletUseCases.reconcileWallet(walletId, correlationId);
  }
}
