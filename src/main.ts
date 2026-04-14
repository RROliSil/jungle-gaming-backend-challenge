import 'reflect-metadata';
import { EntityManager } from '@mikro-orm/core';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { SqsClientService } from './infrastructure/messaging/sqs-client.service';
import { StructuredLogger } from './infrastructure/observability/structured-logger';
import { INITIAL_SCHEMA_UP_SQL } from './infrastructure/persistence/migrations/Migration20261105000001_InitialSchema';
import { ensureRuntimeInfrastructure } from './infrastructure/persistence/runtime-fallback';

async function bootstrap(): Promise<void> {
  const infraMode = await ensureRuntimeInfrastructure();

  const app = await NestFactory.create(AppModule.register(), {
    logger: ['error', 'warn'],
  });

  app.enableCors();
  app.enableShutdownHooks();

  const logger = app.get(StructuredLogger);

  try {
    const em = app.get(EntityManager);
    await em.getConnection().execute(INITIAL_SCHEMA_UP_SQL);
    logger.info('database.migrations.verified', {
      postgresMode: infraMode.postgresMode,
    });
  } catch (err) {
    logger.warn('database.migrations.startup_warning', {
      errorMessage: (err as Error)?.message,
    });
  }

  try {
    const sqs = app.get(SqsClientService);
    sqs.refreshClient();
    await sqs.ensureQueuesExist();
    logger.info('sqs.queues.verified', {
      sqsMode: infraMode.sqsMode,
    });
  } catch (err) {
    logger.warn('sqs.queues.startup_warning', {
      errorMessage: (err as Error)?.message,
    });
  }

  const port = Number(process.env.PORT ?? 3001);
  await app.listen(port);
  logger.info('application.started', {
    port,
    url: `http://localhost:${port}`,
    dashboardUrl: `http://localhost:${port}/dashboard`,
    postgresMode: infraMode.postgresMode,
    sqsMode: infraMode.sqsMode,
  });
}

if (require.main === module) {
  void bootstrap();
}
