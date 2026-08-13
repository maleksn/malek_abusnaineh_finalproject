import type {
  ValidLog,
  LogsQuery,
  AggregateQuery,
} from "../validators/logs.validator";
import { db } from "../db";
import { logs as logsTable } from "../db/schema";
import type { LogCursor } from "../utils/cursor";

import { and, desc, eq, gte, lt, sql } from "drizzle-orm";

type PreparedInsert = {
  execute: (params: Record<string, unknown>) => Promise<unknown>;
};

const preparedInsertCache = new Map<number, PreparedInsert>();

function getPreparedInsert(batchSize: number): PreparedInsert {
  const cached = preparedInsertCache.get(batchSize);

  if (cached !== undefined) {
    return cached;
  }

  const rows = Array.from({ length: batchSize }, (_, index) => ({
    timestamp: sql.placeholder(`timestamp_${index}`),
    level: sql.placeholder(`level_${index}`),
    service: sql.placeholder(`service_${index}`),
    message: sql.placeholder(`message_${index}`),
    attributes: sql.placeholder(`attributes_${index}`),
  }));

  const prepared = db
    .insert(logsTable)
    .values(rows)
    .prepare(`insert_logs_${batchSize}`);

  preparedInsertCache.set(batchSize, prepared);

  return prepared;
}

export async function insertLogs(logs: ValidLog[]): Promise<void> {
  // Just for more security
  if (logs.length === 0) {
    return;
  }

  const prepared = getPreparedInsert(logs.length);

  const params: Record<string, unknown> = {};

  for (const [index, log] of logs.entries()) {
    params[`timestamp_${index}`] = new Date(log.timestamp);
    params[`level_${index}`] = log.level;
    params[`service_${index}`] = log.service;
    params[`message_${index}`] = log.message;
    params[`attributes_${index}`] = log.attributes;
  }

  await prepared.execute(params);
}

export async function queryLogs(
  query: LogsQuery,
  attributeFilters: Record<string, string>,
  cursor?: LogCursor,
) {
  const conditions = [];

  if (query.service !== undefined) {
    conditions.push(eq(logsTable.service, query.service));
  }

  if (query.level !== undefined) {
    conditions.push(eq(logsTable.level, query.level));
  }

  if (query.since !== undefined) {
    conditions.push(gte(logsTable.timestamp, new Date(query.since)));
  }

  if (query.until !== undefined) {
    conditions.push(lt(logsTable.timestamp, new Date(query.until)));
  }

  if (query.q !== undefined) {
    conditions.push(
      sql`LOWER(${logsTable.message}) LIKE LOWER(${"%" + query.q + "%"})`,
    );
  }

  if (cursor !== undefined) {
    const cursorTimestamp = new Date(cursor.timestamp);

    conditions.push(
      sql`(
      ${lt(logsTable.timestamp, cursorTimestamp)}
      OR (
        ${eq(logsTable.timestamp, cursorTimestamp)}
        AND ${lt(logsTable.id, cursor.id)}
      )
    )`,
    );
  }

  for (const [key, value] of Object.entries(attributeFilters)) {
    conditions.push(sql`${logsTable.attributes} ->> ${key} = ${value}`);
  }

  return db
    .select()
    .from(logsTable)
    .where(and(...conditions))
    .orderBy(desc(logsTable.timestamp), desc(logsTable.id))
    .limit(query.limit + 1);
}

function getBucketSeconds(bucket: AggregateQuery["bucket"]): number {
  switch (bucket) {
    case "1m":
      return 60;

    case "5m":
      return 300;

    case "1h":
      return 3600;

    case "1d":
      return 86400;
  }
}

export async function aggregateLogs(
  query: AggregateQuery,
  attributeFilters: Record<string, string>,
) {
  const bucketSeconds = getBucketSeconds(query.bucket);

  const bucketExpression = sql<Date>`
    to_timestamp(
      floor(
        extract(epoch from ${logsTable.timestamp})
        / ${bucketSeconds}
      ) * ${bucketSeconds}
    )
  `;

  const groupExpression =
    query.group_by === "service"
      ? logsTable.service
      : query.group_by === "level"
        ? logsTable.level
        : sql<null>`NULL`;

  const conditions = [
    gte(logsTable.timestamp, new Date(query.since)),
    lt(logsTable.timestamp, new Date(query.until)),
  ];

  if (query.service !== undefined) {
    conditions.push(eq(logsTable.service, query.service));
  }

  if (query.level !== undefined) {
    conditions.push(eq(logsTable.level, query.level));
  }

  if (query.q !== undefined) {
    conditions.push(
      sql`LOWER(${logsTable.message}) LIKE LOWER(${"%" + query.q + "%"})`,
    );
  }

  for (const [key, value] of Object.entries(attributeFilters)) {
    conditions.push(sql`${logsTable.attributes} ->> ${key} = ${value}`);
  }

  return db
    .select({
      start: bucketExpression,
      group: groupExpression,
      count: sql<number>`count(*)::int`,
    })
    .from(logsTable)
    .where(and(...conditions))
    .groupBy(sql.raw("1"), sql.raw("2"))
    .orderBy(sql.raw("1"));
}
