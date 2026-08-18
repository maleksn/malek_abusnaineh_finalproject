import "dotenv/config";
import { Pool } from "pg";
import { drizzle } from "drizzle-orm/node-postgres";
import { migrate } from "drizzle-orm/node-postgres/migrator";

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL!,
  max: 40, // pool size
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

pool.on("error", (err) => {
  console.error("Unexpected error on idle database client:", err);
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