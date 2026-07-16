# fetch-sse-client

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

## 설계

두 층으로 나뉜다:

- **`parseSSE(stream)`** — 순수 SSE 스펙 파서. 청크가 임의 바이트 지점에서 잘려도(멀티바이트 UTF-8 포함) 재조립한다. 네트워크 없이 테스트된다.
- **`connectSSE(url, opts)`** — 전송층. fetch(POST·헤더·취소) → `parseSSE`에 위임 + 재접속(`Last-Event-ID`·백오프) + stall 워치독.

## 개발

```bash
pnpm install
pnpm test
```

## License

MIT
