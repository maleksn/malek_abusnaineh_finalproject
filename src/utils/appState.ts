// =========================================================================
// APPLICATION READINESS STATE: Tracks if the app is ready to serve traffic
// =========================================================================

// Flag indicating whether the database is connected and migrations are done
let isReady = false;

/**
 * Marks the application as READY (e.g. after database is connected).
 * Returns true if the status changed from not ready to ready.
 */
export function markAppReady(): boolean {
  if (isReady) {
    return false;
  }

  isReady = true;
  return true;
}

/**
 * Marks the application as NOT READY (e.g. if database connection dropped).
 * Returns true if the status changed from ready to not ready.
 */
export function markAppNotReady(): boolean {
  if (!isReady) {
    return false;
  }

  isReady = false;
  return true;
}

/**
 * DATA FLOW: Used by GET /health to report whether the app is ready (200 OK) or not ready (503).
 */
export function checkAppReady(): boolean {
  return isReady;
}