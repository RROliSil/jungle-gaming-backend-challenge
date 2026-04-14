import { MikroORM } from '@mikro-orm/postgresql';
import config from '../mikro-orm.config';
import {
  INITIAL_SCHEMA_DOWN_SQL,
  INITIAL_SCHEMA_UP_SQL,
} from '../src/infrastructure/persistence/migrations/Migration20261105000001_InitialSchema';

async function run(): Promise<void> {
  const direction = process.argv[2] ?? 'up';
  const orm = await MikroORM.init(config);
  try {
    const conn = orm.em.getConnection();
    if (direction === 'down') {
      await conn.execute(INITIAL_SCHEMA_DOWN_SQL);
      console.log('Migration DOWN executed successfully.');
    } else {
      await conn.execute(INITIAL_SCHEMA_UP_SQL);
      console.log('Migration UP executed successfully.');
    }
  } finally {
    await orm.close(true);
  }
}

void run();
