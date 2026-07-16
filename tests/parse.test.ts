import { describe, expect, it } from "vitest";
import { parseSSE } from "../src/parse";
import type { SSEvent } from "../src/types";

const enc = new TextEncoder();

/** 주어진 바이트 청크들을 순서대로 흘리고 닫는 ReadableStream. */
function streamOf(chunks: Uint8Array[]): ReadableStream<Uint8Array> {
  return new ReadableStream({
    start(controller) {
      for (const c of chunks) controller.enqueue(c);
      controller.close();
    },
  });
}

async function collect(stream: ReadableStream<Uint8Array>): Promise<SSEvent[]> {
  const events: SSEvent[] = [];
  for await (const ev of parseSSE(stream)) events.push(ev);
  return events;
}

describe("parseSSE", () => {
  it("청크 경계에서 잘린 이벤트를 재조립한다", async () => {
    // JSON 페이로드가 두 청크 사이에서 토큰 중간에 잘림.
    const events = await collect(streamOf([enc.encode('data: {"a":'), enc.encode("1}\n\n")]));
    expect(events).toEqual([{ event: "message", data: '{"a":1}' }]);
  });

  it("청크 경계에서 쪼개진 멀티바이트 UTF-8 문자를 디코딩한다", async () => {
    // "가" = EA B0 80; "data: "(6바이트) + 가의 첫 바이트 뒤에서 자름.
    const bytes = enc.encode("data: 가\n\n");
    const events = await collect(streamOf([bytes.slice(0, 7), bytes.slice(7)]));
    expect(events).toEqual([{ event: "message", data: "가" }]);
  });

  it("여러 data: 줄을 개행으로 잇는다 (스펙)", async () => {
    const events = await collect(streamOf([enc.encode("data: l1\ndata: l2\n\n")]));
    expect(events).toEqual([{ event: "message", data: "l1\nl2" }]);
  });

  it("주석(keep-alive) 줄을 무시한다", async () => {
    const events = await collect(streamOf([enc.encode(": keep-alive\n\ndata: hi\n\n")]));
    expect(events).toEqual([{ event: "message", data: "hi" }]);
  });

  it("event · id · retry 필드를 담는다", async () => {
    const events = await collect(
      streamOf([enc.encode("event: ping\nid: 42\nretry: 3000\ndata: x\n\n")]),
    );
    expect(events).toEqual([{ event: "ping", data: "x", id: "42", retry: 3000 }]);
  });
});
