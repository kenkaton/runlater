# runlater

> Hand work off now. Run it later, reliably.

`runlater` is a small **durable handoff primitive** for Go applications.

It is intentionally not another worker framework. Your cloud already has durable execution systems such as Google Cloud Tasks. `runlater` gives application code one small contract for handing work to those systems without importing their SDK types, worker runtimes, or infrastructure concerns.

```go
receipt, err := later.Do(
    ctx,
    "email.send",
    EmailPayload{UserID: 42},
    runlater.ID("welcome-email:42"),
)
```

The first production backend is Google Cloud Tasks over REST: no gRPC, no protobuf, no Google Cloud Go SDK, and no worker daemon.

## Why this exists

Go already has excellent background-job libraries. If you want Redis/Postgres-backed workers, retries, cron, dashboards, workflow primitives, or queue ownership inside your application, use those libraries.

`runlater` targets a narrower problem:

> **I already trust my cloud to execute durable background work. I only want a tiny, testable boundary in my Go application for handing work to it.**

That leads to a deliberately different architecture:

```text
application
    |
    | runlater.Do(...)
    v
runlater handoff contract
    |
    +---- Cloud Tasks (first backend)
    +---- other cloud-native durable backends only when semantics fit

provider invokes HTTP target
    |
    v
httpjob.Mux
    |
    v
typed Go handler
```

### Non-goals

`runlater` does **not** aim to become:

- a Redis/Postgres job server
- a worker-pool framework
- a workflow engine
- a generic message-broker abstraction
- a lowest-common-denominator wrapper over every queue product

Provider differences that affect correctness stay visible in backend documentation.

## The contract

The root package defines only the handoff semantics:

```go
type Job struct {
    ID      string
    Name    string
    Payload json.RawMessage
    RunAt   time.Time
}

type Dispatcher interface {
    Dispatch(context.Context, Job) (Receipt, error)
}
```

A successful `Dispatch` means the backend has accepted responsibility for the job according to that backend's documented guarantees.

**Durability is a backend guarantee, not a lie told by the interface.** For example, the `memory` dispatcher is useful for tests but is explicitly not durable.

## Stable job IDs and safe retries

Every job has a logical ID. `runlater` generates one by default, but important jobs should usually provide a stable ID:

```go
receipt, err := later.Do(
    ctx,
    "email.send",
    payload,
    runlater.ID("welcome-email:"+userID),
)
```

The Cloud Tasks backend maps that logical ID to a deterministic Cloud Tasks task name. Repeating the same handoff ID therefore maps to the same provider task, and `ALREADY_EXISTS` is treated as an idempotent success.

This addresses a common distributed-systems failure mode: the provider accepted a task, but the client lost the response and cannot tell whether retrying will duplicate the enqueue.

This does **not** provide exactly-once execution. Cloud Tasks is at-least-once. **Handlers must be idempotent.**

## Versioned wire protocol

HTTP-style backends use a small versioned envelope:

```json
{
  "version": 1,
  "id": "welcome-email:42",
  "name": "email.send",
  "payload": {
    "user_id": 42
  }
}
```

Versioning the envelope early keeps producers and consumers evolvable without coupling application code to provider payload formats.

## Receiving jobs

`httpjob` is the other half of the primitive. It routes the wire protocol to typed Go handlers using only `net/http`.

```go
mux := httpjob.New()

err := httpjob.HandleJSON(mux, "email.send", func(ctx context.Context, p EmailPayload) error {
    return sendEmail(ctx, p.UserID)
})
if err != nil {
    log.Fatal(err)
}

http.Handle("/internal/jobs", mux)
```

Malformed envelopes and unknown jobs return 4xx responses. Handler failures return 500 so durable HTTP backends can retry according to their policies.

## Google Cloud Tasks

```go
dispatcher, err := cloudtasks.New(cloudtasks.Config{
    Project:   "my-project",
    Location:  "asia-northeast1",
    Queue:     "default",
    TargetURL: "https://my-service.run.app/internal/jobs",
})
if err != nil {
    return err
}

later := runlater.New(dispatcher)

_, err = later.Do(ctx, "email.send", EmailPayload{UserID: 42})
```

On Cloud Run, the backend uses the metadata server for the attached service account's OAuth token by default.

Authenticated targets can use task-level OIDC configuration, or preferably queue-level target/auth configuration when you want IAM concerns kept out of application code.

### Delayed execution

```go
_, err := later.Do(ctx, "email.send", payload, runlater.After(5*time.Minute))
```

or:

```go
_, err := later.Do(ctx, "email.send", payload, runlater.At(runAt))
```

The Cloud Tasks backend validates provider-specific constraints such as its scheduling horizon and task-size limit.

## Testing

No emulator, credentials, Redis, or worker process is required for unit tests.

```go
mem := &memory.Dispatcher{}
later := runlater.New(mem)

_, _ = later.Do(ctx, "email.send", payload, runlater.ID("test-job"))

jobs := mem.Jobs()
```

## Dependency policy

The module currently has **zero third-party dependencies**.

This is a product constraint, not a stunt. The original motivation for `runlater` was that a tiny background handoff should not introduce gRPC, protobuf, a large cloud SDK dependency graph, unrelated vulnerability alerts, or additional worker infrastructure.

Backends should remain standard-library-first where doing so does not compromise correctness.

## How it differs from existing Go job libraries

Libraries such as Asynq and gocraft/work own a worker runtime and use Redis. Neoq abstracts multiple queue/storage backends and also provides job processing features. River owns durable jobs in Postgres. Those are good choices when the application wants to own the job system.

`runlater` deliberately owns much less:

| Concern | runlater | Traditional Go job framework |
| --- | --- | --- |
| Durable store | Cloud/provider owns it | Library/app owns it |
| Worker runtime | Provider invokes target | Library runs workers |
| Redis/Postgres required | No | Often |
| Cloud SDK types in app | No | N/A |
| Provider-native retry/rate limiting | Reused | Reimplemented/configured by framework |
| Main abstraction | Durable handoff | Queue + workers |
| Third-party dependencies | 0 today | Usually several |

The project should only add a backend when it can preserve the handoff contract without pretending important semantic differences do not exist.

## Status

Pre-v1. The API is intentionally still being challenged before the first stable release.

## License

MIT
