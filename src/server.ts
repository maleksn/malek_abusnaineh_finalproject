import app from "./app";
import { checkDatabaseConnection, runMigrations } from "./db";
import { markAppReady, markAppNotReady } from "./utils/appState";
import { startRetentionWorker } from "./services/retention.service";
 
process.on("unhandledRejection", (reason) => {
  console.error("Unhandled Rejection:", reason);
});
process.on("uncaughtException", (error) => {
  console.error("Uncaught Exception:", error);
});

const PORT = 8080;

const RETRY_INTERVAL = 10000;

// monitor the database connection forever
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

    await new Promise((resolve) =>
      setTimeout(resolve, RETRY_INTERVAL),
    );
  }
}

// for startup only
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

      await new Promise((resolve) =>
        setTimeout(resolve, RETRY_INTERVAL),
      );
    }
  }
}


async function startServer(): Promise<void> {
  app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
  });

  console.log("Waiting for database...");
  await waitForDatabase();

  console.log("Running database migrations...");
  await runMigrations();
  console.log("Database migrations completed.");

  markAppReady();
  console.log("Application is ready.");

  startRetentionWorker();

  await monitorDatabase();
}

startServer();