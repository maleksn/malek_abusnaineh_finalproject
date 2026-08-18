import "dotenv/config";
import { Pool } from "pg";
import { drizzle } from "drizzle-orm/node-postgres";
import { migrate } from "drizzle-orm/node-postgres/migrator";

// Dedicated write pool for high-throughput ingestion workers
export const pool = new Pool({
  connectionString: process.env.DATABASE_URL!,
  max: 3,
  idleTimeoutMillis: 30000,
});

pool.on("error", (err) => {
  console.error("Unexpected error on idle write database client:", err);
});

// Dedicated read pool isolated from ingestion writes to prevent connection starvation
export const readPool = new Pool({
  connectionString: process.env.DATABASE_URL!,
  max: 4,
  idleTimeoutMillis: 30000,
  statement_timeout: 3000,
});

readPool.on("error", (err) => {
  console.error("Unexpected error on idle read database client:", err);
});

export const db = drizzle(pool);

export async function checkDatabaseConnection(): Promise<void> {
  await Promise.all([
    pool.query("SELECT 1"),
    readPool.query("SELECT 1"),
    readPool.query("SELECT 1"),
    readPool.query("SELECT 1"),
  ]);
}

export async function runMigrations(): Promise<void> {
  await migrate(db, {
    migrationsFolder: "./drizzle",
  });
}