import type { SSEvent } from "./types";

/**
 * 바이트 스트림을 Server-Sent Events로 파싱한다 (WHATWG event stream 포맷).
 *
 * 핵심 난점: 청크는 임의의 바이트 지점에서 잘린다 — 이벤트가 필드 중간에서,
 * 심지어 멀티바이트 문자 중간에서도 쪼개질 수 있다. 스트리밍 디코딩 + 청크 간
 * 버퍼링으로, 빈 줄이 이벤트를 끝맺을 때만 dispatch한다.
 */
export async function* parseSSE(stream: ReadableStream<Uint8Array>): AsyncGenerator<SSEvent> {
  const reader = stream.getReader();
  const decoder = new TextDecoder(); // UTF-8, 스트리밍 — 아래 decode({ stream: true }) 참고.
  let buffer = "";

  // 지금 만들고 있는 이벤트의 누적 필드.
  let event = "";
  let data: string[] = [];
  let id: string | undefined; // 스펙상 이벤트 간 유지되는 값("마지막 event ID").
  let retry: number | undefined;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      // { stream: true }는 완성 안 된 마지막 코드포인트를 그 바이트가 다 올 때까지
      // 붙들어둬서, 청크 경계에서 쪼개진 멀티바이트 문자가 깨지지 않는다.
      buffer += decoder.decode(value, { stream: true });

      // 완결된 라인을 모두 소진하고, 끝의 미완성 라인은 `buffer`에 남긴다.
      for (let cut = nextLine(buffer); cut; cut = nextLine(buffer)) {
        const { line, rest } = cut;
        buffer = rest;

        if (line === "") {
          // 빈 줄 = dispatch. 이벤트가 실제로 data를 담았을 때만 내보낸다;
          // 주석/keep-alive만 있는 블록은 이벤트를 만들지 않는다.
          if (data.length > 0) {
            yield { event: event || "message", data: data.join("\n"), id, retry };
          }
          event = "";
          data = [];
          retry = undefined; // id는 일부러 유지 — 지속되는 last-event-id.
          continue;
        }
        if (line.startsWith(":")) continue; // 주석(keep-alive) — 무시.

        // 첫 콜론으로 `field: value` 분리. 콜론이 없으면 라인 전체가 field.
        const colon = line.indexOf(":");
        const field = colon === -1 ? line : line.slice(0, colon);
        let val = colon === -1 ? "" : line.slice(colon + 1);
        if (val.startsWith(" ")) val = val.slice(1); // 앞 공백은 딱 1개만 제거.

        switch (field) {
          case "event":
            event = val;
            break;
          case "data":
            data.push(val);
            break;
          case "id":
            if (!val.includes("\0")) id = val; // 스펙: NUL 포함 id는 무시.
            break;
          case "retry":
            if (/^\d+$/.test(val)) retry = Number(val);
            break;
          // 알 수 없는 필드는 무시(스펙).
        }
      }
    }
    // 스트림 종료: 끝맺는 빈 줄 없이 끝난 마지막 이벤트는 버린다(스펙).
  } finally {
    reader.cancel().catch(() => {}); // 조기 return/abort 시 정리; abort는 삼킨다.
  }
}

/**
 * 첫 완결 라인을 잘라낸다. SSE 종결자 3종(CRLF, CR, LF)을 인식한다.
 * 아직 완결된 라인이 없으면 `null` — 끝의 외로운 `\r`도 포함(다음 청크에서
 * `\r\n`이 될 수 있어 판단을 미룬다).
 */
function nextLine(buffer: string): { line: string; rest: string } | null {
  for (let i = 0; i < buffer.length; i++) {
    const c = buffer[i];
    if (c === "\n") {
      return { line: buffer.slice(0, i), rest: buffer.slice(i + 1) };
    }
    if (c === "\r") {
      if (i === buffer.length - 1) return null; // \r\n이 청크 경계서 쪼개졌을 수 있음 → 대기.
      const skip = buffer[i + 1] === "\n" ? 2 : 1;
      return { line: buffer.slice(0, i), rest: buffer.slice(i + skip) };
    }
  }
  return null;
}
