import { pool } from "../db";

export interface RetentionConfig {
  enabled: boolean;
  retentionDays: number;
  checkIntervalMs: number;
  batchSize: number;
}

export interface RetentionStatus {
  enabled: boolean;
  retentionDays: number;
  checkIntervalMs: number;
  lastRunTime: string | null;
  lastDeletedCount: number;
  totalDeletedLifetime: number;
  isRunning: boolean;
}

const DEFAULT_RETENTION_DAYS = 30;
const DEFAULT_INTERVAL_MS = 3600000; // 1 hour
const PURGE_BATCH_SIZE = 5000;

export const retentionConfig: RetentionConfig = {
  enabled: process.env.RETENTION_ENABLED !== "false",
  retentionDays: Math.max(1, parseInt(process.env.RETENTION_DAYS || String(DEFAULT_RETENTION_DAYS), 10)),
  checkIntervalMs: Math.max(10000, parseInt(process.env.RETENTION_CHECK_INTERVAL_MS || String(DEFAULT_INTERVAL_MS), 10)),
  batchSize: PURGE_BATCH_SIZE,
};

let retentionTimer: NodeJS.Timeout | null = null;
let isPurgeRunning = false;
let lastRunTimestamp: string | null = null;
let lastDeletedCount = 0;
let totalDeletedCountLifetime = 0;

/**
 * Non-blocking batch deletion of expired logs using B-tree index scan
 */
export async function purgeExpiredLogs(
  days: number = retentionConfig.retentionDays,
  batchSize: number = retentionConfig.batchSize
): Promise<{ deletedCount: number; durationMs: number }> {
  if (isPurgeRunning) {
    return { deletedCount: 0, durationMs: 0 };
  }

  isPurgeRunning = true;
  const startTime = Date.now();
  let totalDeletedInRun = 0;

  const cutoffDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

  try {
    const client = await pool.connect();
    try {
      let continuePurging = true;

      while (continuePurging) {
        // Purge up to batchSize rows using indexed timestamp lookup
        const result = await client.query<{ id: number }>(
          `WITH expired AS (
             SELECT id FROM logs
             WHERE timestamp < $1
             ORDER BY timestamp ASC, id ASC
             LIMIT $2
           )
           DELETE FROM logs
           WHERE id IN (SELECT id FROM expired)
           RETURNING id`,
          [cutoffDate.toISOString(), batchSize]
        );

        const count = result.rowCount || 0;
        totalDeletedInRun += count;

        if (count < batchSize) {
          continuePurging = false;
        } else {
          // Yield 50ms to allow concurrent write queries and autovacuum breathing room
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
      }
    } finally {
      client.release();
    }
  } catch (error) {
    console.error("Log retention purge error:", error);
  } finally {
    isPurgeRunning = false;
    lastRunTimestamp = new Date().toISOString();
    lastDeletedCount = totalDeletedInRun;
    totalDeletedCountLifetime += totalDeletedInRun;
  }

  const durationMs = Date.now() - startTime;
  return { deletedCount: totalDeletedInRun, durationMs };
}

/**
 * Start the background retention scheduler
 */
export function startRetentionWorker(): void {
  if (!retentionConfig.enabled) {
    console.log("[Retention] Background retention service disabled via config.");
    return;
  }

  if (retentionTimer) {
    clearInterval(retentionTimer);
  }

  console.log(
    `[Retention] Starting background retention worker (Policy: ${retentionConfig.retentionDays} days, Interval: ${retentionConfig.checkIntervalMs / 1000}s)`
  );

  retentionTimer = setInterval(async () => {
    try {
      const { deletedCount, durationMs } = await purgeExpiredLogs();
      if (deletedCount > 0) {
        console.log(
          `[Retention] Background purge completed: Deleted ${deletedCount} expired logs in ${durationMs}ms.`
        );
      }
    } catch (err) {
      console.error("[Retention] Scheduled purge failed:", err);
    }
  }, retentionConfig.checkIntervalMs);
}

/**
 * Stop the background retention scheduler
 */
export function stopRetentionWorker(): void {
  if (retentionTimer) {
    clearInterval(retentionTimer);
    retentionTimer = null;
  }
}

/**
 * Get current retention status metadata
 */
export function getRetentionStatus(): RetentionStatus {
  return {
    enabled: retentionConfig.enabled,
    retentionDays: retentionConfig.retentionDays,
    checkIntervalMs: retentionConfig.checkIntervalMs,
    lastRunTime: lastRunTimestamp,
    lastDeletedCount,
    totalDeletedLifetime: totalDeletedCountLifetime,
    isRunning: isPurgeRunning,
  };
}
