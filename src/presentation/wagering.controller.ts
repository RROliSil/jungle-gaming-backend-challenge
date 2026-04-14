import {
  Body,
  Controller,
  Get,
  Headers,
  HttpStatus,
  Param,
  Post,
  Res,
} from '@nestjs/common';
import {
  ProcessWagerTransactionUseCase,
  WagerTransactionResultDto,
} from '../application/use-cases/process-wager-transaction.use-case';
import { InvalidTransactionPropsError } from '../domain/shared/domain-errors';
import { MoneyProps } from '../domain/shared/money';
import {
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../domain/wagering/wager-transaction';

@Controller()
export class WageringController {
  constructor(
    private readonly processWagerUseCase: ProcessWagerTransactionUseCase,
  ) {}

  @Post('wagering/transactions')
  async submitTransaction(
    @Headers('idempotency-key') idempotencyKeyHeader: string | undefined,
    @Headers('x-correlation-id') correlationIdHeader: string | undefined,
    @Body()
    body: {
      providerId: string;
      externalTransactionId: string;
      playerId: string;
      walletId: string;
      roundId: string;
      gameId: string;
      kind: WagerTransactionKind;
      money: MoneyProps;
      referenceExternalTransactionId?: string;
    },
    @Res({ passthrough: true })
    res: { status: (code: number) => void },
  ): Promise<WagerTransactionResultDto> {
    if (!idempotencyKeyHeader || idempotencyKeyHeader.trim().length === 0) {
      throw new InvalidTransactionPropsError(
        'MISSING_IDEMPOTENCY_KEY',
        'Header Idempotency-Key is mandatory',
      );
    }

    if (!body || typeof body !== 'object') {
      throw new InvalidTransactionPropsError(
        'INVALID_PAYLOAD',
        'Request body must be a valid JSON object',
      );
    }

    const result = await this.processWagerUseCase.execute({
      idempotencyKey: idempotencyKeyHeader.trim(),
      providerId: body.providerId,
      externalTransactionId: body.externalTransactionId,
      playerId: body.playerId,
      walletId: body.walletId,
      roundId: body.roundId,
      gameId: body.gameId,
      kind: body.kind,
      money: body.money,
      referenceExternalTransactionId: body.referenceExternalTransactionId,
      correlationId: correlationIdHeader,
    });

    // Section 9: Clear HTTP status distinction between PROCESSED (200),
    // PENDING_REFERENCE (202), and REJECTED business rule violations (422)
    if (result.status === WagerTransactionStatus.PendingReference) {
      res.status(HttpStatus.ACCEPTED);
    } else if (result.status === WagerTransactionStatus.Rejected) {
      res.status(HttpStatus.UNPROCESSABLE_ENTITY);
    } else {
      res.status(HttpStatus.OK);
    }

    return result;
  }

  @Get('wagering/transactions/:transactionId')
  async getTransactionById(
    @Param('transactionId') transactionId: string,
  ): Promise<WagerTransactionResultDto> {
    return this.processWagerUseCase.getByTransactionId(transactionId);
  }

  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  async getTransactionByProviderAndExternalId(
    @Param('providerId') providerId: string,
    @Param('externalTransactionId') externalTransactionId: string,
  ): Promise<WagerTransactionResultDto> {
    return this.processWagerUseCase.getByProviderAndExternalId(
      providerId,
      externalTransactionId,
    );
  }
}
