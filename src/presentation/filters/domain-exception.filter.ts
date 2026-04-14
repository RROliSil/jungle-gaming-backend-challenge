import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
} from '@nestjs/common';
import {
  CurrencyMismatchError,
  DomainError,
  DuplicateWalletError,
  EntityNotFoundError,
  IdempotencyConflictError,
  InsufficientFundsError,
  InvalidMoneyError,
  InvalidTransactionPropsError,
  InvalidTransactionStateError,
  LedgerImbalanceError,
} from '../../domain/shared/domain-errors';
import { StructuredLogger } from '../../infrastructure/observability/structured-logger';

@Catch()
export class DomainExceptionFilter implements ExceptionFilter {
  constructor(private readonly logger: StructuredLogger) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<{
      status: (code: number) => { json: (body: Record<string, unknown>) => void };
    }>();
    const request = ctx.getRequest<{
      url?: string;
      method?: string;
      headers?: Record<string, string | undefined>;
    }>();

    const correlationId =
      request?.headers?.['x-correlation-id'] ??
      request?.headers?.['X-Correlation-Id'] ??
      undefined;

    if (exception instanceof HttpException) {
      const status = exception.getStatus();
      const resBody = exception.getResponse();
      const payload =
        typeof resBody === 'object' && resBody !== null
          ? (resBody as Record<string, unknown>)
          : { code: 'HTTP_ERROR', message: String(resBody) };

      response.status(status).json({
        statusCode: status,
        timestamp: new Date().toISOString(),
        path: request?.url,
        ...payload,
      });
      return;
    }

    if (
      exception instanceof IdempotencyConflictError ||
      exception instanceof DuplicateWalletError
    ) {
      response.status(HttpStatus.CONFLICT).json({
        statusCode: HttpStatus.CONFLICT,
        code: exception.code,
        error: 'Conflict',
        message: exception.message,
        timestamp: new Date().toISOString(),
      });
      return;
    }

    if (exception instanceof EntityNotFoundError) {
      response.status(HttpStatus.NOT_FOUND).json({
        statusCode: HttpStatus.NOT_FOUND,
        code: exception.code,
        error: 'Not Found',
        message: exception.message,
        timestamp: new Date().toISOString(),
      });
      return;
    }

    if (
      exception instanceof InvalidMoneyError ||
      exception instanceof InvalidTransactionPropsError
    ) {
      response.status(HttpStatus.BAD_REQUEST).json({
        statusCode: HttpStatus.BAD_REQUEST,
        code: exception.code,
        error: 'Bad Request',
        message: exception.message,
        timestamp: new Date().toISOString(),
      });
      return;
    }

    if (
      exception instanceof InsufficientFundsError ||
      exception instanceof CurrencyMismatchError ||
      exception instanceof InvalidTransactionStateError ||
      exception instanceof LedgerImbalanceError
    ) {
      response.status(HttpStatus.UNPROCESSABLE_ENTITY).json({
        statusCode: HttpStatus.UNPROCESSABLE_ENTITY,
        code: exception.code,
        failureCode: exception.code,
        error: 'Unprocessable Entity',
        message: exception.message,
        timestamp: new Date().toISOString(),
      });
      return;
    }

    if (exception instanceof DomainError) {
      if (
        exception.code === 'PROVIDER_ALREADY_EXISTS' ||
        exception.code === 'GAME_ALREADY_EXISTS'
      ) {
        response.status(HttpStatus.CONFLICT).json({
          statusCode: HttpStatus.CONFLICT,
          code: exception.code,
          error: 'Conflict',
          message: exception.message,
          timestamp: new Date().toISOString(),
        });
        return;
      }

      if (
        exception.code === 'WALLET_NON_ZERO_BALANCE' ||
        exception.code === 'WALLET_NOT_ACTIVE'
      ) {
        response.status(HttpStatus.UNPROCESSABLE_ENTITY).json({
          statusCode: HttpStatus.UNPROCESSABLE_ENTITY,
          code: exception.code,
          error: 'Unprocessable Entity',
          message: exception.message,
          timestamp: new Date().toISOString(),
        });
        return;
      }

      response.status(HttpStatus.BAD_REQUEST).json({
        statusCode: HttpStatus.BAD_REQUEST,
        code: exception.code,
        error: 'Bad Request',
        message: exception.message,
        timestamp: new Date().toISOString(),
      });
      return;
    }

    const errMessage = (exception as Error)?.message ?? 'Internal error';
    if (errMessage === 'INVALID_LEDGER_CURSOR') {
      response.status(HttpStatus.BAD_REQUEST).json({
        statusCode: HttpStatus.BAD_REQUEST,
        code: 'INVALID_LEDGER_CURSOR',
        error: 'Bad Request',
        message: 'Provided ledger pagination cursor is malformed or invalid',
        timestamp: new Date().toISOString(),
      });
      return;
    }

    // Transient infrastructure errors -> 503 Service Unavailable so provider knows to retry
    this.logger.error('http.unhandled_infrastructure_error', {
      correlationId,
      path: request?.url,
      method: request?.method,
      errorMessage: errMessage,
    });

    response.status(HttpStatus.SERVICE_UNAVAILABLE).json({
      statusCode: HttpStatus.SERVICE_UNAVAILABLE,
      code: 'TRANSIENT_INFRASTRUCTURE_ERROR',
      error: 'Service Unavailable',
      message: 'A transient infrastructure error occurred; safe to retry with the same Idempotency-Key',
      detail: errMessage,
      timestamp: new Date().toISOString(),
    });
  }
}
