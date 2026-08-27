# runlater

> Run work later. Reliably.

`runlater` is a tiny durable-background-work primitive for Go.

The goal is to keep application code focused on intent:

```go
err := later.Do(ctx, "send-welcome-email", payload)
```

while a backend handles durable delivery. The first production backend is Google Cloud Tasks over its REST API — without gRPC, protobuf, or the Google Cloud Go SDK.

## Status

Early development. The v0.1 API is being built now.

## Design goals

- Small, explicit Go API
- Durable, at-least-once background execution semantics
- Standard-library-first implementation
- Cloud-provider details stay outside application code
- Easy local and unit testing
- No worker daemon required for serverless backends

## License

MIT
