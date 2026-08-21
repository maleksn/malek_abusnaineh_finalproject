import "dotenv/config";
import { Pool, types } from "pg";
import { drizzle } from "drizzle-orm/node-postgres";
import { migrate } from "drizzle-orm/node-postgres/migrator";

// =========================================================================
// DATA CONVERSION HELPERS: Fast translation from Postgres types to JavaScript
// =========================================================================

// Convert 64-bit big numbers from Postgres directly to standard JavaScript numbers
types.setTypeParser(20, (val: string) => Number(val));

// Convert Postgres timestamp strings to clean ISO-8601 UTC date strings (e.g. '2026-08-18T19:40:00Z')
types.setTypeParser(1184, (val: string) => {
  if (!val) return val;
  let s = val.indexOf("T") === -1 ? val.replace(" ", "T") : val;
  if (s.endsWith("+00") || s.endsWith("+00:00")) {
    return s.replace(/\+00(:00)?$/, "Z");
  }
  if (/[+-]\d{2}$/.test(s)) {
    return s + ":00";
  }
  return s;
});

// Convert timestamp without timezone to standard UTC ISO string
types.setTypeParser(1114, (val: string) => {
  if (!val) return val;
  return val.indexOf("T") === -1 ? val.replace(" ", "T") + "Z" : val;
});

// =========================================================================
// DATABASE CONNECTIONS: Separate pools for Writing and Reading
// =========================================================================

// WRITE POOL: Used exclusively for inserting new logs quickly
export const pool = new Pool({
  connectionString: process.env.DATABASE_URL!,
  max: 4,
  idleTimeoutMillis: 30000,
});

pool.on("error", (err) => {
  console.error("Unexpected error on idle write database client:", err);
});

// READ POOL: Used exclusively for search queries and dashboards so heavy writes don't slow down reads
export const readPool = new Pool({
  connectionString: process.env.DATABASE_URL!,
  max: 4,
  idleTimeoutMillis: 30000,
  statement_timeout: 5000,
});

readPool.on("error", (err) => {
  console.error("Unexpected error on idle read database client:", err);
});

export const db = drizzle(pool);

/**
 * DATA FLOW: Tests whether both Write and Read connections to the database are responsive.
 */
export async function checkDatabaseConnection(): Promise<void> {
  await Promise.all([
    pool.query("SELECT 1"),
    readPool.query({
      name: "check_db_read",
      text: "SELECT 1",
    }),
  ]);
}

/**
 * DATA FLOW: Applies database schema migrations (creates tables and indexes) on application startup.
 */
export async function runMigrations(): Promise<void> {
  try {
    await migrate(db, {
      migrationsFolder: "./drizzle",
    });
  } catch (err: unknown) {
    const error = err as { cause?: { code?: string } };
    if (error?.cause?.code === "42710") {
      return;
    }
    throw err;
  }
}