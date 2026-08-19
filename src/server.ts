import app from "./app";
import { checkDatabaseConnection, runMigrations } from "./db";
import { markAppReady, markAppNotReady } from "./utils/appState";
import { startRetentionWorker } from "./services/retention.service";

// =========================================================================
// APPLICATION ENTRY POINT: Starts HTTP server and connects to database
// =========================================================================

// Safety listeners to log any unexpected background errors
process.on("unhandledRejection", (reason) => {
  console.error("Unhandled Rejection:", reason);
});
process.on("uncaughtException", (error) => {
  console.error("Uncaught Exception:", error);
});

const PORT = 8080;
const RETRY_INTERVAL = 10000; // 10 seconds

/**
 * DATA FLOW: Continuously checks the database health in the background every 10 seconds.
 * If the database goes down, marks the app as NOT READY.
 * When the database comes back up, marks the app as READY.
 */
async function monitorDatabase(): Promise<void> {
  while (true) {
    try {
      await checkDatabaseConnection();

      const becameReady = markAppReady();
      if (becameReady) {
        console.log("Database is available. Application is ready.");
      }
    } catch (error) {
      const becameNotReady = markAppNotReady();
      if (becameNotReady) {
        console.error(
          "Database became unavailable. Application is not ready.",
        );

        if (error instanceof Error) {
          console.error(error.message);
        }
      }
    }

    // Wait 10 seconds before next health check
    await new Promise((resolve) =>
      setTimeout(resolve, RETRY_INTERVAL),
    );
  }
}

/**
 * DATA FLOW: Loops during startup until PostgreSQL is reachable.
 */
async function waitForDatabase(): Promise<void> {
  while (true) {
    try {
      await checkDatabaseConnection();
      console.log("Database is available.");
      return;
    } catch (error) {
      console.error("Database is not available yet.");

      if (error instanceof Error) {
        console.error(error.message);
      }

      console.log(
        `Retrying in ${RETRY_INTERVAL / 1000} seconds...`,
      );

      // Wait 10 seconds before retrying
      await new Promise((resolve) =>
        setTimeout(resolve, RETRY_INTERVAL),
      );
    }
  }
}

/**
 * DATA FLOW (Server Startup Sequence):
 * Step 1: Start the Express HTTP server on port 8080.
 * Step 2: Wait until PostgreSQL database is online and reachable.
 * Step 3: Automatically apply any pending database table migrations.
 * Step 4: Mark application status as READY (GET /health will now return 200 OK).
 * Step 5: Start background log retention worker to delete old logs.
 * Step 6: Start background database health monitor.
 */
async function startServer(): Promise<void> {
  // Step 1: Listen for HTTP requests
  const server = app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
  });

  // Optimize HTTP keep-alive timeouts for high throughput
  server.keepAliveTimeout = 65000;
  server.headersTimeout = 66000;
  server.maxRequestsPerSocket = 0;

  // Step 2: Connect to database
  console.log("Waiting for database...");
  await waitForDatabase();

  // Step 3: Run table migrations
  console.log("Running database migrations...");
  await runMigrations();
  console.log("Database migrations completed.");

  // Step 4: Mark app as ready
  markAppReady();
  console.log("Application is ready.");

  // Step 5: Start background retention cleanup worker
  startRetentionWorker();

  // Step 6: Start continuous database health monitoring
  await monitorDatabase();
}

startServer();