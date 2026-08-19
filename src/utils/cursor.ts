// =========================================================================
// PAGINATION CURSOR: Bookmarks for paging through large log datasets
// =========================================================================

// A bookmark pointing to the last log seen on the current page
export type LogCursor = {
  timestamp: string; // The exact time of the last log
  id: number;        // The unique database ID of the last log
};

/**
 * DATA FLOW:
 * Step 1: Takes the last log's timestamp and ID.
 * Step 2: Converts it into a compact Base64URL string.
 * Step 3: Returns it to the client as "next_cursor".
 */
export function encodeCursor(cursor: LogCursor): string {
  return Buffer.from(JSON.stringify(cursor)).toString("base64url");
}

/**
 * DATA FLOW:
 * Step 1: Receives the "next_cursor" string from the client's URL query.
 * Step 2: Decodes and validates the Base64URL string.
 * Step 3: Returns the original { timestamp, id } so the database query knows where to start.
 */
export function decodeCursor(value: string): LogCursor {
  let parsed: unknown;

  try {
    parsed = JSON.parse(Buffer.from(value, "base64url").toString("utf8"));
  } catch {
    throw new Error("Invalid cursor");
  }

  // Validate that the decoded object has both timestamp and positive numeric id
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

  // Validate that timestamp is a valid date
  if (Number.isNaN(new Date(parsed.timestamp).getTime())) {
    throw new Error("Invalid cursor");
  }

  return {
    timestamp: parsed.timestamp,
    id: parsed.id,
  };
}
