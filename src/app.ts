import express from "express";
import { checkAppReady } from "./utils/appState";
import logsRouter from "./routes/logs.routes";
import { errorHandler } from "./middleware/errorHandler";

const app = express();
app.disable("x-powered-by");
app.disable("etag");

// Middleware
app.use(express.json({ limit: "1mb" }));

app.use("/logs", logsRouter);

// Temporary health endpoint
app.get("/health", (_req, res) => {
  if (!checkAppReady()) {
    return res.status(503).json({
      status: "not_ready",
    });
  }
  

  return res.status(200).json({
    status: "ok",
  });
});

app.use(errorHandler);

export default app;