# fetch-sse-client

[![CI](https://github.com/eomdh/fetch-sse-client/actions/workflows/ci.yml/badge.svg)](https://github.com/eomdh/fetch-sse-client/actions/workflows/ci.yml)

`fetch` + `ReadableStream`로 SSE(Server-Sent Events)를 직접 파싱하는 클라이언트. **EventSource가 아니다.**

```ts
const ac = new AbortController();

for await (const ev of connectSSE("/stream", {
  method: "POST",
  body: JSON.stringify({ q: "안녕" }),
  headers: { Authorization: `Bearer ${token}` },
  signal: ac.signal,
})) {
  const msg = JSON.parse(ev.data);
}
ac.abort(); // 취소
```

## 왜 EventSource가 아니라

`EventSource`는 **GET 전용 · 요청 바디 불가 · 커스텀 헤더 불가**(그래서 `Authorization`을 못 실음) · 취소가 빈약하다. `fetch` 위에 SSE를 직접 구현하면 **POST 바디 · 헤더 주입 · `AbortController` 취소**를 얻는 대신, 파싱과 재접속을 직접 책임진다.

## 설계 — 두 층

- **`parseSSE(stream)`** — 순수 SSE 스펙 파서. 청크가 임의 바이트 지점에서 잘려도(멀티바이트 UTF-8 포함) 재조립한다. 네트워크 없이 테스트된다.
- **`connectSSE(url, opts)`** — 전송층. fetch(POST·헤더·취소) → `parseSSE`에 위임 + 재접속(`Last-Event-ID`·서버 `retry` 존중·지수 백오프) + stall 워치독.

### 옵션 (`connectSSE`)

`RequestInit`(`method`/`body`/`headers` …)에 더해:

| 옵션 | 기본 | 뜻 |
|---|---|---|
| `signal` | — | `AbortController` 취소 |
| `reconnect` | `true` | 끊기면 재접속 |
| `reconnectDelay` | `1000` | 재접속 기준 지연(ms) |
| `maxReconnectDelay` | `30000` | 지수 백오프 상한(ms) |
| `stallTimeout` | `0`(끔) | 이 시간 무수신 시 abort & 재접속 |

## 설계 결정 · 트레이드오프

- **파서/전송 2층 분리** — 파서는 순수함수(결정적 테스트), 전송은 재접속·타임아웃 관심사. 제일 어려운 파서를 네트워크 목킹 없이 지저분한 청크 경계로 직접 때린다.
- **async generator API** — `for await`로 소비, 취소는 `break` 또는 `signal`. 파서·전송이 같은 `AsyncGenerator<SSEvent>` 추상으로 일관된다.
- **청크 경계 버퍼링 + 스트리밍 디코딩** — `TextDecoder({ stream: true })`로 멀티바이트 분할을 막고, 완결 라인만 처리하고 미완성 라인은 버퍼에 남긴다(가장 흔한 스트림 파싱 버그).
- **stall 워치독 = 스트림 에러** — 타임아웃 시 pass-through `TransformStream`이 `controller.error()`로 스트림을 끊는다. 소켓 abort 전파에 기대지 않아 어떤 런타임에서도 파서가 즉시 풀린다.
- **재접속** — `Last-Event-ID`로 이어받고, 서버 `retry:`를 기준 지연으로 존중하며, 연속 실패에 지수 백오프(상한 이내). 성과 있는 연결에서 백오프를 리셋한다.

## 한계 · 로드맵

- **EventSource 전체 API 호환 아님** — `readyState`/`addEventListener` 스타일 없음(async-generator 설계로 의도적 대체).
- **`StallError`는 export 안 함** — `reconnect: false`일 때 던져지지만 `instanceof` 대신 `err.name === "StallError"`로 판별.
- **최대 재시도 횟수 제한 없음** — abort 전까지 재접속(백오프 상한만 있음).
- **환경 전제** — WHATWG `fetch`/스트림(Node 18+ · 모던 브라우저). 폴리필 없음. npm 미배포.

## 개발

```bash
pnpm install
pnpm test        # vitest (파서 5 + 전송 5)
pnpm typecheck   # tsc
pnpm lint        # biome
```

## License

MIT
