import "dotenv/config";
import { Pool, types } from "pg";
import { drizzle } from "drizzle-orm/node-postgres";
import { migrate } from "drizzle-orm/node-postgres/migrator";

// High-performance type parsers:
// OID 20: int8 / bigserial / count(*) as JavaScript number
types.setTypeParser(20, (val: string) => Number(val));
// OID 1184: timestamptz - format to standard ISO-8601 string without Date object overhead
types.setTypeParser(1184, (val: string) => {
  if (!val) return val;
  // Convert Postgres timestamptz text (e.g. '2026-08-18 19:40:00.123+00') to ISO-8601
  let s = val.indexOf("T") === -1 ? val.replace(" ", "T") : val;
  if (s.endsWith("+00") || s.endsWith("+00:00")) {
    return s.replace(/\+00(:00)?$/, "Z");
  }
  if (/[+-]\d{2}$/.test(s)) {
    return s + ":00";
  }
  return s;
});
// OID 1114: timestamp without timezone
types.setTypeParser(1114, (val: string) => {
  if (!val) return val;
  return val.indexOf("T") === -1 ? val.replace(" ", "T") + "Z" : val;
});

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
    readPool.query({
      name: "check_db_read",
      text: "SELECT 1",
    }),
  ]);
}

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