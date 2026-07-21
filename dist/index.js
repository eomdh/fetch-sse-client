// src/parse.ts
async function* parseSSE(stream) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let event = "";
  let data = [];
  let id;
  let retry;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      for (let cut = nextLine(buffer); cut; cut = nextLine(buffer)) {
        const { line, rest } = cut;
        buffer = rest;
        if (line === "") {
          if (data.length > 0) {
            yield { event: event || "message", data: data.join("\n"), id, retry };
          }
          event = "";
          data = [];
          retry = void 0;
          continue;
        }
        if (line.startsWith(":")) continue;
        const colon = line.indexOf(":");
        const field = colon === -1 ? line : line.slice(0, colon);
        let val = colon === -1 ? "" : line.slice(colon + 1);
        if (val.startsWith(" ")) val = val.slice(1);
        switch (field) {
          case "event":
            event = val;
            break;
          case "data":
            data.push(val);
            break;
          case "id":
            if (!val.includes("\0")) id = val;
            break;
          case "retry":
            if (/^\d+$/.test(val)) retry = Number(val);
            break;
        }
      }
    }
  } finally {
    reader.cancel().catch(() => {
    });
  }
}
function nextLine(buffer) {
  for (let i = 0; i < buffer.length; i++) {
    const c = buffer[i];
    if (c === "\n") {
      return { line: buffer.slice(0, i), rest: buffer.slice(i + 1) };
    }
    if (c === "\r") {
      if (i === buffer.length - 1) return null;
      const skip = buffer[i + 1] === "\n" ? 2 : 1;
      return { line: buffer.slice(0, i), rest: buffer.slice(i + skip) };
    }
  }
  return null;
}

// src/connect.ts
var StallError = class extends Error {
  constructor(ms) {
    super(`SSE stream stalled: no data for ${ms}ms`);
    this.name = "StallError";
  }
};
async function* connectSSE(input, options = {}) {
  const {
    signal: userSignal,
    reconnect = true,
    reconnectDelay = 1e3,
    stallTimeout = 0,
    maxReconnectDelay = 3e4,
    headers,
    ...init
  } = options;
  let lastEventId;
  let retryMs = reconnectDelay;
  let failures = 0;
  while (true) {
    if (userSignal?.aborted) return;
    const attempt = new AbortController();
    const onUserAbort = () => attempt.abort();
    userSignal?.addEventListener("abort", onUserAbort, { once: true });
    try {
      const requestHeaders = new Headers(headers);
      if (!requestHeaders.has("Accept")) requestHeaders.set("Accept", "text/event-stream");
      if (lastEventId !== void 0) requestHeaders.set("Last-Event-ID", lastEventId);
      const res = await fetch(input, {
        ...init,
        headers: requestHeaders,
        signal: attempt.signal
      });
      if (!res.ok || !res.body) {
        throw new Error(`SSE request failed with status ${res.status}`);
      }
      const body = stallTimeout > 0 ? watchStall(res.body, stallTimeout) : res.body;
      let productive = false;
      for await (const ev of parseSSE(body)) {
        if (userSignal?.aborted) return;
        if (ev.id !== void 0) lastEventId = ev.id;
        if (ev.retry !== void 0) retryMs = ev.retry;
        if (!productive) {
          productive = true;
          failures = 0;
        }
        yield ev;
        if (userSignal?.aborted) return;
      }
    } catch (err) {
      if (userSignal?.aborted) return;
      if (!reconnect) throw err;
    } finally {
      userSignal?.removeEventListener("abort", onUserAbort);
      attempt.abort();
    }
    if (userSignal?.aborted || !reconnect) return;
    const wait = Math.min(retryMs * 2 ** failures, maxReconnectDelay);
    failures += 1;
    await delay(wait, userSignal);
  }
}
function watchStall(body, ms) {
  let timer;
  const arm = (controller) => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      try {
        controller.error(new StallError(ms));
      } catch {
      }
    }, ms);
  };
  return body.pipeThrough(
    new TransformStream({
      start: (controller) => arm(controller),
      transform(chunk, controller) {
        arm(controller);
        controller.enqueue(chunk);
      },
      flush: () => clearTimeout(timer)
    })
  );
}
function delay(ms, signal) {
  return new Promise((resolve) => {
    if (signal?.aborted) return resolve();
    const onAbort = () => {
      clearTimeout(timer);
      resolve();
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

export { connectSSE, parseSSE };
