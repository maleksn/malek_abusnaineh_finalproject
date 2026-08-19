import type { Request } from "express";

// =========================================================================
// ATTRIBUTE FILTER EXTRACTOR: Finds custom attribute filters in URL queries
// =========================================================================

/**
 * DATA FLOW:
 * Step 1: Scan all query parameters in the request URL (e.g. ?service=auth&attr.env=prod&attr.ip=1.2.3.4).
 * Step 2: Pick only parameters that start with "attr.".
 * Step 3: Remove the "attr." prefix and group them into a clean key-value object (e.g. { env: "prod", ip: "1.2.3.4" }).
 * Step 4: Return the filters so the database can filter by these JSON fields.
 */
export function extractAttributeFilters(
  query: Request["query"],
): Record<string, string> {
  const attributeFilters: Record<string, string> = {};

  for (const [key, value] of Object.entries(query)) {
    // Only process parameters starting with "attr."
    if (!key.startsWith("attr.")) {
      continue;
    }

    if (typeof value !== "string") {
      continue;
    }

    // Strip "attr." prefix to get the actual attribute name
    const attributeKey = key.slice(5);

    if (attributeKey.length === 0) {
      continue;
    }

    attributeFilters[attributeKey] = value;
  }

  return attributeFilters;
}
