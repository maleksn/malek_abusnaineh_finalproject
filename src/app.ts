import express from "express";
import { checkAppReady } from "./utils/appState";
import logsRouter from "./routes/logs.routes";
import { errorHandler } from "./middleware/errorHandler";

// =========================================================================
// EXPRESS APPLICATION SETUP: Main HTTP router and middleware
// =========================================================================

const app = express();

// Disable unnecessary HTTP headers for maximum performance
app.disable("x-powered-by");
app.disable("etag");
app.set("query parser", "simple");

// Step 1: Parse incoming JSON request bodies (up to 1 MB)
app.use(express.json({ limit: "1mb" }));

// Step 2: Route all /logs requests (POST /logs, GET /logs, GET /logs/aggregate)
app.use("/logs", logsRouter);

// Step 3: Health check endpoint (GET /health) for container orchestrators (e.g. Docker, Kubernetes)
app.get("/health", (_req, res) => {
  if (!checkAppReady()) {
    // Database is not yet ready or unavailable
    return res.status(503).json({
      status: "not_ready",
    });
  }

  // Database is ready and server is accepting traffic
  return res.status(200).json({
    status: "ok",
  });
});

// Step 4: Catch-all error handler middleware
app.use(errorHandler);

export default app;