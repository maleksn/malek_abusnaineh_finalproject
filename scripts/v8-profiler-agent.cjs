const http = require("node:http");
const fs = require("node:fs");

function getJson(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        try {
          resolve(JSON.parse(data));
        } catch (e) {
          reject(e);
        }
      });
    }).on("error", reject);
  });
}

async function main() {
  const targets = await getJson("http://127.0.0.1:9229/json");
  if (!targets || targets.length === 0) {
    throw new Error("No inspector target found");
  }

  const wsUrl = targets[0].webSocketDebuggerUrl;
  const ws = new WebSocket(wsUrl);

  let messageId = 1;
  const pending = new Map();

  function send(method, params = {}) {
    return new Promise((resolve) => {
      const id = messageId++;
      pending.set(id, resolve);
      ws.send(JSON.stringify({ id, method, params }));
    });
  }

  ws.onmessage = (event) => {
    const msg = JSON.parse(event.data);
    if (msg.id && pending.has(msg.id)) {
      const resolve = pending.get(msg.id);
      pending.delete(msg.id);
      resolve(msg.result);
    }
  };

  await new Promise((resolve) => (ws.onopen = resolve));

  await send("Profiler.enable");
  await send("Profiler.setSamplingInterval", { interval: 100 });
  await send("Profiler.start");

  console.log("PROFILER_STARTED");

  let stopping = false;
  async function stopProfiling() {
    if (stopping) return;
    stopping = true;

    const result = await send("Profiler.stop");
    fs.writeFileSync("/app/v8-profile.json", JSON.stringify(result.profile));
    console.log("PROFILER_STOPPED");
    ws.close();
    process.exit(0);
  }

  process.on("SIGTERM", stopProfiling);
  process.on("SIGINT", stopProfiling);
  process.on("message", (msg) => {
    if (msg === "stop") stopProfiling();
  });
}

main().catch((err) => {
  console.error("Profiler Agent Error:", err);
  process.exit(1);
});
