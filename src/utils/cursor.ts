export type LogCursor = {
  timestamp: string;
  id: number;
};

export function encodeCursor(cursor: LogCursor): string {
  return Buffer.from(JSON.stringify(cursor)).toString("base64url");
}

export function decodeCursor(value: string): LogCursor {
  let parsed: unknown;

  try {
    parsed = JSON.parse(Buffer.from(value, "base64url").toString("utf8"));
  } catch {
    throw new Error("Invalid cursor");
  }

  if (
    typeof parsed !== "object" ||
    parsed === null ||
    !("timestamp" in parsed) ||
    !("id" in parsed) ||
    typeof parsed.timestamp !== "string" ||
    typeof parsed.id !== "number" ||
    parsed.id < 1
  ) {
    throw new Error("Invalid cursor");
  }

  if (Number.isNaN(new Date(parsed.timestamp).getTime())) {
    throw new Error("Invalid cursor");
  }

  return {
    timestamp: parsed.timestamp,
    id: parsed.id,
  };
}
