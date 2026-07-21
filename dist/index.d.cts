/** 파싱된 Server-Sent Event (WHATWG event stream 포맷). */
interface SSEvent {
    /** `event:` 필드. 서버가 생략하면 `"message"`가 기본값. */
    event: string;
    /** 페이로드 — `data:` 필드 줄들을 `"\n"`으로 이은 값. */
    data: string;
    /** `id:` 필드(있을 때). 재접속 시 `Last-Event-ID`의 근거가 된다. */
    id?: string;
    /** `retry:` 필드(ms, 있을 때). */
    retry?: number;
}

/** {@link connectSSE} 옵션. fetch의 `RequestInit`을 확장한다. */
interface ConnectSSEOptions extends Omit<RequestInit, "signal"> {
    /** 이 시그널이 발화하면 연결을 abort하고 yield를 멈춘다. */
    signal?: AbortSignal;
    /** 스트림이 끝나거나 에러나면 재접속. 기본값 `true`. */
    reconnect?: boolean;
    /** 재접속 시도 간 기본 지연(ms). 기본값 `1000`. */
    reconnectDelay?: number;
    /** 이 시간(ms) 동안 바이트가 없으면 abort & 재접속. `0`은 비활성. 기본값 `0`. */
    stallTimeout?: number;
    /** 지수 백오프 상한(ms). 기본값 `30000`. */
    maxReconnectDelay?: number;
}
/**
 * fetch 위에서 SSE 연결을 열고 파싱된 이벤트를 하나씩 yield한다.
 *
 * 왜 EventSource가 아니라: EventSource는 GET 전용 + 요청 바디 불가 + 커스텀
 * 헤더(auth/tenant) 불가 + 취소가 빈약하다. fetch 위에 얹으면 POST 바디 · 헤더
 * 주입 · AbortController 취소를 얻는 대신, 파싱({@link parseSSE})과 재접속을
 * 직접 책임진다.
 */
declare function connectSSE(input: string | URL, options?: ConnectSSEOptions): AsyncGenerator<SSEvent>;

/**
 * 바이트 스트림을 Server-Sent Events로 파싱한다 (WHATWG event stream 포맷).
 *
 * 핵심 난점: 청크는 임의의 바이트 지점에서 잘린다 — 이벤트가 필드 중간에서,
 * 심지어 멀티바이트 문자 중간에서도 쪼개질 수 있다. 스트리밍 디코딩 + 청크 간
 * 버퍼링으로, 빈 줄이 이벤트를 끝맺을 때만 dispatch한다.
 */
declare function parseSSE(stream: ReadableStream<Uint8Array>): AsyncGenerator<SSEvent>;

export { type ConnectSSEOptions, type SSEvent, connectSSE, parseSSE };
