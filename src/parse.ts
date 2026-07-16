import type { SSEvent } from "./types";

/**
 * 바이트 스트림을 Server-Sent Events로 파싱한다 (WHATWG event stream 포맷).
 *
 * 핵심 난점: 청크는 임의의 바이트 지점에서 잘린다 — 이벤트가 필드 중간에서,
 * 심지어 멀티바이트 문자 중간에서도 쪼개질 수 있다. 스트리밍 디코딩 + 청크 간
 * 버퍼링으로, 빈 줄이 이벤트를 끝맺을 때만 dispatch한다.
 *
 * TODO(core): 스펙 파서 구현.
 *   1. `new TextDecoder()`를 스트리밍 모드(`{ stream: true }`)로 디코딩 —
 *      청크 경계서 쪼개진 멀티바이트 문자가 깨지지 않게.
 *   2. 디코딩한 텍스트를 버퍼링; CRLF / CR / LF로 라인 분리.
 *   3. 이벤트별 필드 누적: `event`, `data`(여러 `data:`는 "\n"으로 조인), `id`, `retry`.
 *      ":"로 시작하는 줄은 주석 — 무시.
 *   4. 빈 줄에서 이벤트 dispatch(data 있을 때만); 버퍼 리셋.
 *   5. 필드 콜론 뒤 앞 공백 1개 제거(`data: x` -> `x`).
 */
// biome-ignore lint/correctness/useYield: 스텁 — 코어는 다음 커밋에서 구현.
export async function* parseSSE(_stream: ReadableStream<Uint8Array>): AsyncGenerator<SSEvent> {
  throw new Error("parseSSE: not implemented");
}
