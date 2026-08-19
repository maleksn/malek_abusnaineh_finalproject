import type { ErrorRequestHandler } from "express";

// =========================================================================
// GLOBAL ERROR HANDLER: Catches and formats any errors in the application
// =========================================================================

// Helper to check if an error is caused by invalid/broken JSON sent by client
function isMalformedJsonError(
  error: unknown,
): error is SyntaxError & {
  status: number;
} {
  return (
    error instanceof SyntaxError &&
    "status" in error &&
    error.status === 400
  );
}

/**
 * DATA FLOW:
 * Step 1: An error happens anywhere during a request.
 * Step 2: If the error is broken JSON syntax -> return HTTP 400 "Malformed JSON".
 * Step 3: If the error is server overload (backpressure) -> return HTTP 503 with Retry-After header.
 * Step 4: For any other unexpected error -> log details and return HTTP 500 "Internal server error".
 */
export const errorHandler: ErrorRequestHandler = (
  error,
  _req,
  res,
  _next,
) => {
  // Case 1: Broken JSON syntax from client
  if (isMalformedJsonError(error)) {
    return res.status(400).json({
      error: "Malformed JSON",
    });
  }

  // Case 2: System under high load, asking client to retry in 1 second
  if (error instanceof Error && error.name === "BackpressureError") {
    res.set("Retry-After", "1");
    return res.status(503).json({
      error: error.message,
    });
  }

  // Case 3: Unexpected internal system error
  console.error("Unhandled error:", error);

  return res.status(500).json({
    error: "Internal server error",
  });
};


