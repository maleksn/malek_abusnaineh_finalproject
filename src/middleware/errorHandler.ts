import type {
  ErrorRequestHandler,
} from "express";

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

export const errorHandler: ErrorRequestHandler = (
  error,
  _req,
  res,
  _next,
) => {
  if (isMalformedJsonError(error)) {
    return res.status(400).json({
      error: "Malformed JSON",
    });
  }

  console.error(error);

  return res.status(500).json({
    error: "Internal server error",
  });
};