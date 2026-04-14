import { MikroOrmModule } from '@mikro-orm/nestjs';
import { PostgreSqlDriver } from '@mikro-orm/postgresql';
import { DynamicModule, Module } from '@nestjs/common';
import { APP_FILTER, APP_GUARD } from '@nestjs/core';
import {
  EVENT_PUBLISHER_PORT,
  PROVIDER_IDENTITY_PORT,
  REALTIME_NOTIFIER_PORT,
} from './application/ports/application.ports';
import { CatalogCrudUseCases } from './application/use-cases/catalog-crud.use-cases';
import { ProcessWagerTransactionUseCase } from './application/use-cases/process-wager-transaction.use-case';
import { WalletUseCases } from './application/use-cases/wallet.use-cases';
import {
  KeycloakOidcIdentityAdapter,
  OidcAuthGuard,
} from './infrastructure/auth/oidc-auth.guard';
import { OutboxWorker } from './infrastructure/messaging/outbox.worker';
import { PendingReferenceWorker } from './infrastructure/messaging/pending-reference.worker';
import { SqsClientService } from './infrastructure/messaging/sqs-client.service';
import { SqsWagerConsumer } from './infrastructure/messaging/sqs-wager.consumer';
import { MetricsService } from './infrastructure/observability/metrics.service';
import { StructuredLogger } from './infrastructure/observability/structured-logger';
import { ALL_ORM_SCHEMAS } from './infrastructure/persistence/entities/orm-entities';
import { WageringWebsocketGateway } from './infrastructure/websocket/wagering.gateway';
import { CatalogController } from './presentation/catalog.controller';
import { DomainExceptionFilter } from './presentation/filters/domain-exception.filter';
import { HealthMetricsController } from './presentation/health-metrics.controller';
import { WageringController } from './presentation/wagering.controller';
import { WalletsController } from './presentation/wallets.controller';

export interface AppModuleOptions {
  dbHost?: string;
  dbPort?: number;
  dbUser?: string;
  dbPassword?: string;
  dbName?: string;
}

@Module({})
export class AppModule {
  static register(options?: AppModuleOptions): DynamicModule {
    return {
      module: AppModule,
      imports: [
        MikroOrmModule.forRoot({
          driver: PostgreSqlDriver,
          entities: ALL_ORM_SCHEMAS,
          host: options?.dbHost ?? process.env.DB_HOST ?? 'localhost',
          port: Number(options?.dbPort ?? process.env.DB_PORT ?? 5432),
          user: options?.dbUser ?? process.env.DB_USER ?? 'jungle',
          password:
            options?.dbPassword ?? process.env.DB_PASSWORD ?? 'jungle_secret',
          dbName: options?.dbName ?? process.env.DB_NAME ?? 'jungle_wagering',
          allowGlobalContext: true,
          pool: {
            min: 1,
            max: 20,
          },
        }),
      ],
      controllers: [
        WalletsController,
        WageringController,
        CatalogController,
        HealthMetricsController,
      ],
      providers: [
        StructuredLogger,
        MetricsService,
        SqsClientService,
        WageringWebsocketGateway,
        KeycloakOidcIdentityAdapter,
        {
          provide: EVENT_PUBLISHER_PORT,
          useExisting: SqsClientService,
        },
        {
          provide: REALTIME_NOTIFIER_PORT,
          useExisting: WageringWebsocketGateway,
        },
        {
          provide: PROVIDER_IDENTITY_PORT,
          useExisting: KeycloakOidcIdentityAdapter,
        },
        {
          provide: APP_GUARD,
          useClass: OidcAuthGuard,
        },
        {
          provide: APP_FILTER,
          useClass: DomainExceptionFilter,
        },
        WalletUseCases,
        ProcessWagerTransactionUseCase,
        CatalogCrudUseCases,
        SqsWagerConsumer,
        OutboxWorker,
        PendingReferenceWorker,
      ],
      exports: [
        StructuredLogger,
        MetricsService,
        SqsClientService,
        WageringWebsocketGateway,
        WalletUseCases,
        ProcessWagerTransactionUseCase,
        CatalogCrudUseCases,
        SqsWagerConsumer,
        OutboxWorker,
        PendingReferenceWorker,
      ],
    };
  }
}
