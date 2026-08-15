import "dotenv/config";
import { Pool } from "pg";
import { drizzle } from "drizzle-orm/node-postgres";
import { migrate } from "drizzle-orm/node-postgres/migrator";

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL!,
  max: 10, // pool size
});

export const db = drizzle(pool);

export async function checkDatabaseConnection(): Promise<void> {
  await pool.query("SELECT 1");
}

export async function runMigrations(): Promise<void> {
  await migrate(db, {
    migrationsFolder: "./drizzle",
  });
}