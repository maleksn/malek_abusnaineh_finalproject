import { pool } from "../db";

// =========================================================================
// RETENTION SERVICE: Automatically cleans up old logs after a period of time
// =========================================================================

// Configuration settings for log cleanup
export interface RetentionConfig {
  enabled: boolean;        // Whether automatic cleanup is turned on
  retentionDays: number;   // Number of days to keep logs before deleting
  checkIntervalMs: number; // How often to check for old logs (in milliseconds)
  batchSize: number;       // How many logs to delete in one go (batch)
}

// Current status and history of log cleanup
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
const PURGE_BATCH_SIZE = 5000;       // Delete 5,000 logs per step to prevent DB lag

// Read settings from environment variables or use sensible defaults
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
 * DATA FLOW: Deletes expired logs in small safe batches.
 * Step 1: Calculate the cutoff date (e.g. Current Time minus 30 days).
 * Step 2: Select the oldest 5,000 logs that are older than the cutoff date.
 * Step 3: Delete those logs from PostgreSQL.
 * Step 4: If 5,000 logs were deleted, pause 50ms and repeat until no more old logs remain.
 * Step 5: Record how many logs were deleted and how long it took.
 */
export async function purgeExpiredLogs(
  days: number = retentionConfig.retentionDays,
  batchSize: number = retentionConfig.batchSize
): Promise<{ deletedCount: number; durationMs: number }> {
  // Prevent two cleanup processes from running at the same time
  if (isPurgeRunning) {
    return { deletedCount: 0, durationMs: 0 };
  }

  isPurgeRunning = true;
  const startTime = Date.now();
  let totalDeletedInRun = 0;

  // Step 1: Calculate cutoff date
  const cutoffDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

  try {
    const client = await pool.connect();
    try {
      let continuePurging = true;

      // Step 2 & 3: Delete in batches
      while (continuePurging) {
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

        // If we deleted fewer than the batch size, we are done
        if (count < batchSize) {
          continuePurging = false;
        } else {
          // Step 4: Small 50ms pause to give database breathing room for other operations
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
      }
    } finally {
      client.release();
    }
  } catch (error) {
    console.error("Log retention purge error:", error);
  } finally {
    // Step 5: Save cleanup metrics
    isPurgeRunning = false;
    lastRunTimestamp = new Date().toISOString();
    lastDeletedCount = totalDeletedInRun;
    totalDeletedCountLifetime += totalDeletedInRun;
  }

  const durationMs = Date.now() - startTime;
  return { deletedCount: totalDeletedInRun, durationMs };
}

/**
 * DATA FLOW: Starts the recurring background timer to run log cleanup periodically.
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

  // Run cleanup every checkIntervalMs (e.g. every hour)
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
 * Stops the recurring background cleanup timer (e.g. during server shutdown).
 */
export function stopRetentionWorker(): void {
  if (retentionTimer) {
    clearInterval(retentionTimer);
    retentionTimer = null;
  }
}

/**
 * DATA FLOW: Returns current statistics about log retention for monitoring.
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
