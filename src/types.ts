/** 파싱된 Server-Sent Event (WHATWG event stream 포맷). */
export interface SSEvent {
  /** `event:` 필드. 서버가 생략하면 `"message"`가 기본값. */
  event: string;
  /** 페이로드 — `data:` 필드 줄들을 `"\n"`으로 이은 값. */
  data: string;
  /** `id:` 필드(있을 때). 재접속 시 `Last-Event-ID`의 근거가 된다. */
  id?: string;
  /** `retry:` 필드(ms, 있을 때). */
  retry?: number;
}
