import type { Request } from "express";

export function extractAttributeFilters(
  query: Request["query"],
): Record<string, string> {
  const attributeFilters: Record<string, string> = {};

  for (const [key, value] of Object.entries(query)) {
    if (!key.startsWith("attr.")) {
      continue;
    }

    if (typeof value !== "string") {
      continue;
    }

    const attributeKey = key.slice(5);

    if (attributeKey.length === 0) {
      continue;
    }

    attributeFilters[attributeKey] = value;
  }

  return attributeFilters;
}
