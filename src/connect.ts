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

/**
 * fetch 위에서 SSE 연결을 열고 파싱된 이벤트를 하나씩 yield한다.
 *
 * 왜 EventSource가 아니라: EventSource는 GET 전용 + 요청 바디 불가 + 커스텀
 * 헤더(auth/tenant) 불가 + 취소가 빈약하다. fetch 위에 얹으면 POST 바디 · 헤더
 * 주입 · AbortController 취소를 얻는 대신, 파싱({@link parseSSE})과 재접속을
 * 직접 책임진다.
 *
 * TODO(core): 전송 구현.
 *   1. `fetch(input, { ...init, signal })` 후 `res.body`를 `parseSSE`에 넘긴다.
 *   2. 마지막 이벤트 `id`를 추적; 재접속 시 `Last-Event-ID`로 보낸다.
 *   3. 스트림 종료(서버 드롭) 후: `reconnect`이고 abort 안 됐으면
 *      `reconnectDelay`(백오프) 대기 후 재연결.
 *   4. `signal` 존중: abort되면 아무것도 yield 안 함; fetch 정리.
 *   5. `stallTimeout`: 그 시간 안에 바이트 없으면 abort & 재접속.
 */
// biome-ignore lint/correctness/useYield: 스텁 — 전송은 다음 커밋에서 구현.
export async function* connectSSE(
  _input: string | URL,
  _options: ConnectSSEOptions = {},
): AsyncGenerator<SSEvent> {
  throw new Error("connectSSE: not implemented");
}
