import { defineConfig } from '@mikro-orm/postgresql';
import { Migrator } from '@mikro-orm/migrations';
import { ALL_ORM_SCHEMAS } from './src/infrastructure/persistence/entities/orm-entities';
import { Migration20261105000001_InitialSchema } from './src/infrastructure/persistence/migrations/Migration20261105000001_InitialSchema';

export default defineConfig({
  entities: ALL_ORM_SCHEMAS,
  host: process.env.DB_HOST ?? 'localhost',
  port: Number(process.env.DB_PORT ?? 5432),
  user: process.env.DB_USER ?? 'jungle',
  password: process.env.DB_PASSWORD ?? 'jungle_secret',
  dbName: process.env.DB_NAME ?? 'jungle_wagering',
  extensions: [Migrator],
  migrations: {
    migrationsList: [
      {
        name: 'Migration20261105000001_InitialSchema',
        class: Migration20261105000001_InitialSchema,
      },
    ],
  },
  pool: {
    min: 2,
    max: 25,
  },
});
