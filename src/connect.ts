import { parseSSE } from "./parse";
import type { SSEvent } from "./types";

/** {@link connectSSE} 옵션. fetch의 `RequestInit`을 확장한다. */
export interface ConnectSSEOptions extends Omit<RequestInit, "signal"> {
  /** 이 시그널이 발화하면 연결을 abort하고 yield를 멈춘다. */
  signal?: AbortSignal;
  /** 스트림이 끝나거나 에러나면 재접속. 기본값 `true`. */
  reconnect?: boolean;
  /** 재접속 시도 간 기본 지연(ms). 기본값 `1000`. */
  reconnectDelay?: number;
  /** 이 시간(ms) 동안 바이트가 없으면 abort & 재접속. `0`은 비활성. 기본값 `0`. */
  stallTimeout?: number;
}

/** stall 워치독이 스트림에 던지는 에러; `reconnect`가 꺼졌을 때만 호출자에게 노출된다. */
class StallError extends Error {
  constructor(ms: number) {
    super(`SSE stream stalled: no data for ${ms}ms`);
    this.name = "StallError";
  }
}

/**
 * fetch 위에서 SSE 연결을 열고 파싱된 이벤트를 하나씩 yield한다.
 *
 * 왜 EventSource가 아니라: EventSource는 GET 전용 + 요청 바디 불가 + 커스텀
 * 헤더(auth/tenant) 불가 + 취소가 빈약하다. fetch 위에 얹으면 POST 바디 · 헤더
 * 주입 · AbortController 취소를 얻는 대신, 파싱({@link parseSSE})과 재접속을
 * 직접 책임진다.
 */
export async function* connectSSE(
  input: string | URL,
  options: ConnectSSEOptions = {},
): AsyncGenerator<SSEvent> {
  const {
    signal: userSignal,
    reconnect = true,
    reconnectDelay = 1000,
    stallTimeout = 0,
    headers,
    ...init
  } = options;

  // 재접속 사이에 유지 → `Last-Event-ID`로 이어받는다.
  let lastEventId: string | undefined;

  while (true) {
    if (userSignal?.aborted) return;

    // 시도별 컨트롤러: 사용자 시그널이 전체를 abort하고, 여기서 브리지한다.
    const attempt = new AbortController();
    const onUserAbort = () => attempt.abort();
    userSignal?.addEventListener("abort", onUserAbort, { once: true });

    try {
      const requestHeaders = new Headers(headers as HeadersInit | undefined);
      if (!requestHeaders.has("Accept")) requestHeaders.set("Accept", "text/event-stream");
      if (lastEventId !== undefined) requestHeaders.set("Last-Event-ID", lastEventId);

      const res = await fetch(input, {
        ...init,
        headers: requestHeaders,
        signal: attempt.signal,
      });
      if (!res.ok || !res.body) {
        throw new Error(`SSE request failed with status ${res.status}`);
      }

      // 워치독이 raw 바이트를 관찰해 끊기면 스트림을 에러낸다 — 그래서 이벤트를
      // 안 만드는 SSE keep-alive 주석도 liveness로 세고, 소켓 abort 전파에
      // 기대지 않고 파서가 즉시 풀린다.
      const body = stallTimeout > 0 ? watchStall(res.body, stallTimeout) : res.body;

      for await (const ev of parseSSE(body)) {
        if (userSignal?.aborted) return;
        if (ev.id !== undefined) lastEventId = ev.id;
        yield ev;
        if (userSignal?.aborted) return;
      }
    } catch (err) {
      // 사용자 abort는 정상 종료 — 노출하거나 재시도할 에러가 아니다.
      if (userSignal?.aborted) return;
      // 끊긴/실패한 스트림 또는 stall: 재접속, 비활성이면 다시 던진다.
      if (!reconnect) throw err;
    } finally {
      userSignal?.removeEventListener("abort", onUserAbort);
      attempt.abort(); // 조기 return/에러/재접속 시 fetch를 정리.
    }

    if (userSignal?.aborted || !reconnect) return;
    await delay(reconnectDelay, userSignal);
  }
}

/**
 * 청크를 그대로 통과시키되, `ms` 안에 청크가 안 오면 스트림을
 * {@link StallError}로 에러낸다. 청크마다 재무장, 스트림 종료 시 해제.
 */
function watchStall(body: ReadableStream<Uint8Array>, ms: number): ReadableStream<Uint8Array> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const arm = (controller: TransformStreamDefaultController<Uint8Array>) => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      try {
        controller.error(new StallError(ms));
      } catch {
        // 스트림이 이미 닫힘/취소됨 — 에러낼 대상이 없다.
      }
    }, ms);
  };
  return body.pipeThrough(
    new TransformStream<Uint8Array, Uint8Array>({
      start: (controller) => arm(controller),
      transform(chunk, controller) {
        arm(controller);
        controller.enqueue(chunk);
      },
      flush: () => clearTimeout(timer),
    }),
  );
}

/** `ms`만큼 대기하되, `signal`이 abort하면 (reject가 아니라) 조기 resolve. */
function delay(ms: number, signal?: AbortSignal): Promise<void> {
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
