import process from "node:process";

type LoadTestOptions = {
  url: string;
  rate: number;
  batchSize: number;
  duration: number;
};

type LogResponse = {
  accepted?: number;
  rejected?: unknown[];
};

function getArg(name: string, defaultValue: string): string {
  const index = process.argv.indexOf(name);

  if (index === -1) {
    return defaultValue;
  }

  return process.argv[index + 1] ?? defaultValue;
}

function parseOptions(): LoadTestOptions {
  const url = getArg("--url", "http://localhost:8080/logs");
  const rate = Number(getArg("--rate", "1000"));
  const batchSize = Number(getArg("--batch-size", "100"));
  const duration = Number(getArg("--duration", "10"));

  if (!Number.isFinite(rate) || rate <= 0) {
    throw new Error("--rate must be greater than 0");
  }

  if (!Number.isFinite(batchSize) || batchSize <= 0) {
    throw new Error("--batch-size must be greater than 0");
  }

  if (!Number.isFinite(duration) || duration <= 0) {
    throw new Error("--duration must be greater than 0");
  }

  return {
    url,
    rate,
    batchSize,
    duration,
  };
}

function createLog(index: number) {
  return {
    timestamp: new Date().toISOString(),
    level: "info",
    service: "load-test",
    message: `Load test log ${index}`,
    attributes: {},
  };
}

async function sendBatch(
  url: string,
  batch: ReturnType<typeof createLog>[],
): Promise<{
  accepted: number;
  rejected: number;
}> {
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      logs: batch,
    }),
  });

  let body: LogResponse = {};

  try {
    body = (await response.json()) as LogResponse;
  } catch {
    // Ignore invalid/non-JSON response.
  }

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }

  return {
    accepted: body.accepted ?? 0,
    rejected: body.rejected?.length ?? 0,
  };
}

async function main() {
  const options = parseOptions();

  const totalLogs = Math.floor(options.rate * options.duration);
  const totalBatches = Math.ceil(totalLogs / options.batchSize);

  const intervalMs =
    (options.batchSize / options.rate) * 1000;

  console.log("");
  console.log("Load Test");
  console.log("---------");
  console.log(`URL:          ${options.url}`);
  console.log(`Target rate:  ${options.rate} logs/sec`);
  console.log(`Batch size:   ${options.batchSize} logs/request`);
  console.log(`Duration:     ${options.duration} sec`);
  console.log(`Target logs:  ${totalLogs}`);
  console.log(`Requests:     ${totalBatches}`);
  console.log("");

  let accepted = 0;
  let rejected = 0;
  let errors = 0;
  let completedRequests = 0;

  const startTime = performance.now();

  const requests: Promise<void>[] = [];

  for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
    const remainingLogs =
      totalLogs - batchIndex * options.batchSize;

    const currentBatchSize = Math.min(
      options.batchSize,
      remainingLogs,
    );

    const batch = Array.from(
      { length: currentBatchSize },
      (_, index) =>
        createLog(
          batchIndex * options.batchSize + index,
        ),
    );

    const requestPromise = sendBatch(options.url, batch)
      .then((result) => {
        accepted += result.accepted;
        rejected += result.rejected;
      })
      .catch((error: unknown) => {
        errors++;

        if (error instanceof Error) {
          const cause =
            error.cause instanceof Error
              ? {
                  name: error.cause.name,
                  message: error.cause.message,
                  code: (error.cause as { code?: unknown }).code,
                  errno: (error.cause as { errno?: unknown }).errno,
                  syscall: (error.cause as { syscall?: unknown }).syscall,
                }
              : error.cause;

          console.error("Request failed:", {
            name: error.name,
            message: error.message,
            cause,
          });
        } else {
          console.error("Request failed:", error);
        }
      })
      .finally(() => {
        completedRequests++;
      });

    requests.push(requestPromise);

    if (batchIndex < totalBatches - 1) {
      await new Promise((resolve) =>
        setTimeout(resolve, intervalMs),
      );
    }

    if ((batchIndex + 1) % Math.max(1, Math.floor(totalBatches / 10)) === 0) {
      console.log(
        `Progress: ${batchIndex + 1}/${totalBatches} requests scheduled`,
      );
    }
  }

  await Promise.all(requests);

  const elapsedSeconds =
    (performance.now() - startTime) / 1000;

  const attemptedLogs = totalLogs;

  console.log("");
  console.log("Results");
  console.log("-------");
  console.log(`Elapsed:          ${elapsedSeconds.toFixed(2)} sec`);
  console.log(`Attempted logs:   ${attemptedLogs}`);
  console.log(`Accepted logs:    ${accepted}`);
  console.log(`Rejected logs:    ${rejected}`);
  console.log(`Failed requests:  ${errors}`);
  console.log(`Completed reqs:   ${completedRequests}`);
  console.log(
    `Accepted rate:    ${(accepted / elapsedSeconds).toFixed(2)} logs/sec`,
  );
  console.log(
    `Attempted rate:   ${(attemptedLogs / elapsedSeconds).toFixed(2)} logs/sec`,
  );
  console.log("");
}

main().catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
