import { HttpResponse, http } from "msw";
import { setupServer } from "msw/node";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { connectSSE } from "../src/connect";

const server = setupServer();
beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const enc = new TextEncoder();

/** 주어진 SSE 프레임들을 흘리고 닫는 응답 바디 스트림. */
function sseBody(...frames: string[]): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(c) {
      for (const f of frames) c.enqueue(enc.encode(f));
      c.close();
    },
  });
}

const SSE_HEADERS = { "content-type": "text/event-stream" };

describe("connectSSE", () => {
  it("POST 바디 + 커스텀 헤더를 보내고 파싱된 이벤트를 yield한다", async () => {
    let seenAuth: string | null = null;
    let seenBody: string | null = null;
    server.use(
      http.post("http://x/stream", async ({ request }) => {
        seenAuth = request.headers.get("authorization");
        seenBody = await request.text();
        return new HttpResponse(sseBody("data: one\n\n", "data: two\n\n"), {
          headers: SSE_HEADERS,
        });
      }),
    );

    const data: string[] = [];
    for await (const ev of connectSSE("http://x/stream", {
      method: "POST",
      body: JSON.stringify({ q: "hi" }),
      headers: { authorization: "Bearer t" },
      reconnect: false,
    })) {
      data.push(ev.data);
    }

    expect(seenAuth).toBe("Bearer t");
    expect(seenBody).toBe('{"q":"hi"}');
    expect(data).toEqual(["one", "two"]);
  });

  it("abort() 이후에는 아무것도 yield하지 않는다", async () => {
    server.use(
      http.get(
        "http://x/stream",
        () =>
          new HttpResponse(sseBody("data: 1\n\n", "data: 2\n\n", "data: 3\n\n"), {
            headers: SSE_HEADERS,
          }),
      ),
    );

    const ac = new AbortController();
    const seen: string[] = [];
    for await (const ev of connectSSE("http://x/stream", { signal: ac.signal, reconnect: false })) {
      seen.push(ev.data);
      ac.abort(); // 첫 이벤트 직후 abort.
    }

    expect(seen).toEqual(["1"]); // abort 후엔 "2" / "3" 안 옴.
  });

  it("재접속하며 Last-Event-ID로 이어받는다", async () => {
    let attempt = 0;
    const lastEventIds: (string | null)[] = [];
    server.use(
      http.get("http://x/stream", ({ request }) => {
        lastEventIds.push(request.headers.get("last-event-id"));
        attempt += 1;
        // 첫 연결은 id 5 보내고 끊음(서버 드롭); 두 번째는 이어받음.
        const frame = attempt === 1 ? "id: 5\ndata: a\n\n" : "data: b\n\n";
        return new HttpResponse(sseBody(frame), { headers: SSE_HEADERS });
      }),
    );

    const seen: string[] = [];
    for await (const ev of connectSSE("http://x/stream", { reconnectDelay: 1 })) {
      seen.push(ev.data);
      if (seen.length === 2) break; // a(1차) + b(2차).
    }

    expect(seen).toEqual(["a", "b"]);
    expect(lastEventIds).toEqual([null, "5"]);
  });

  it("스트림이 stall하면 abort하고 재접속한다", async () => {
    let attempt = 0;
    server.use(
      http.get("http://x/stream", () => {
        attempt += 1;
        if (attempt === 1) {
          // 이벤트 하나 보내고 영원히 hang(안 닫음) → 워치독이 발화해야 함.
          return new HttpResponse(
            new ReadableStream({
              start(c) {
                c.enqueue(enc.encode("data: a\n\n"));
                // 일부러 닫지 않음
              },
            }),
            { headers: SSE_HEADERS },
          );
        }
        return new HttpResponse(sseBody("data: b\n\n"), { headers: SSE_HEADERS });
      }),
    );

    const seen: string[] = [];
    for await (const ev of connectSSE("http://x/stream", { stallTimeout: 40, reconnectDelay: 1 })) {
      seen.push(ev.data);
      if (seen.length === 2) break;
    }

    expect(seen).toEqual(["a", "b"]);
    expect(attempt).toBe(2);
  });

  it("서버의 retry: 값을 재접속 지연으로 존중한다", async () => {
    let attempt = 0;
    let firstAt = 0;
    let gap = -1;
    server.use(
      http.get("http://x/stream", () => {
        attempt += 1;
        if (attempt === 1) {
          // retry를 30ms로 지정하고 끊음 — 기본값(1000ms)이 아니라 30ms 뒤 재접속해야 함.
          firstAt = Date.now();
          return new HttpResponse(sseBody("retry: 30\ndata: a\n\n"), { headers: SSE_HEADERS });
        }
        gap = Date.now() - firstAt;
        return new HttpResponse(sseBody("data: b\n\n"), { headers: SSE_HEADERS });
      }),
    );

    const seen: string[] = [];
    for await (const ev of connectSSE("http://x/stream")) {
      seen.push(ev.data);
      if (seen.length === 2) break;
    }

    expect(seen).toEqual(["a", "b"]);
    // retry:30을 존중했으면 간격이 기본 1000ms보다 훨씬 짧다.
    expect(gap).toBeGreaterThanOrEqual(0);
    expect(gap).toBeLessThan(500);
  });
});
