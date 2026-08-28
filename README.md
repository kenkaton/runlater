# runlater

> Hand work off now. Run it later, reliably.

`runlater` is a small **provider-native durable handoff primitive** for Go.

It is intentionally not another background-job framework. Cloud platforms already provide durable execution systems such as Google Cloud Tasks. `runlater` gives application code one small, testable boundary for handing work to those systems without importing provider SDK types, running a worker daemon, or pulling a large dependency graph into the service.

```go
receipt, err := later.Do(
    ctx,
    "email.send",
    EmailPayload{UserID: 42},
    runlater.ID("welcome-email:42"),
)
```

The first production backend is Google Cloud Tasks over REST: no gRPC, no protobuf, no Google Cloud Go SDK, and no worker runtime owned by `runlater`.

## Why this exists

Go already has excellent job systems. Asynq and gocraft/work are good choices when you want Redis-backed workers. River is a good choice when you want jobs owned in Postgres. Neoq is a good choice when you want a broader queue-agnostic job framework.

`runlater` solves a narrower problem:

> **My platform already knows how to durably execute work. I want my Go application to express only the handoff, not own another job system.**

That distinction is the project.

```text
application
    |
    | runlater.Do(...)
    v
runlater handoff contract
    |
    v
provider-managed durable execution
    |
    v
application handler
```

`runlater` should stay thin in features and opinionated in semantics.

## Ownership model

Correctness depends on keeping responsibilities explicit.

| Concern | Owner |
| --- | --- |
| Job identity and handoff intent | `runlater` |
| JSON wire envelope and versioning | `runlater` |
| Provider translation | backend package |
| Durable storage | provider |
| Retry timing / rate limiting / queue policy | provider |
| Authentication / IAM | deployment / provider configuration |
| Business idempotency | application handler |
| Workflow orchestration | application or another system |

A successful `Dispatch` means only this:

> **The selected backend accepted responsibility for the job according to that backend's documented guarantees.**

Durability is therefore a backend property, not something the root interface pretends to guarantee. The `memory` backend is intentionally useful for tests and intentionally not durable.

## Core contract

```go
type Job struct {
    ID      string
    Name    string
    Payload json.RawMessage
    RunAt   time.Time
}

type Receipt struct {
    ID           string
    ProviderID   string
    Deduplicated bool
}

type Dispatcher interface {
    Dispatch(context.Context, Job) (Receipt, error)
}
```

The root package deliberately knows nothing about queues, service accounts, Redis, visibility timeouts, Cloud Tasks protobufs, or SQS message attributes.

## Job identity and retry safety

A job's logical identity is the pair **`(Name, ID)`**. `Name` identifies the kind of work; `ID` identifies one logical occurrence of that work.

If no ID is supplied, `runlater` generates one. A generated ID gives the job a unique identity, but **does not make a repeated call retry-safe**, because the next call receives a different ID.

For an operation that may be retried, provide a stable ID derived from the business operation:

```go
receipt, err := later.Do(
    ctx,
    "email.send",
    payload,
    runlater.ID("welcome-email:"+userID),
)
```

The Cloud Tasks backend deterministically maps `(Name, ID)` to a Cloud Tasks task name. Repeating the same handoff therefore maps to the same provider task, while two different job names may safely reuse the same local ID. When Cloud Tasks explicitly reports `ALREADY_EXISTS`, the backend treats that as an idempotent handoff success and sets `Receipt.Deduplicated`.

A stable ID is an idempotency key for the handoff. **Do not reuse the same `(Name, ID)` for different logical work or different payloads.** Doing so is a caller bug: a provider may legitimately report that the earlier handoff already exists.

### The deduplication window outlives execution

This is the sharpest edge in the model and worth stating plainly.

Cloud Tasks refuses to create a task whose name it has recently seen — **including names of tasks that have already run or been deleted**. That window is roughly one hour for queues created through Cloud Tasks, and roughly nine days for queues created from `queue.yaml`.

So a stable `(Name, ID)` is an identity for **one logical occurrence of work**, not a channel you can send down repeatedly:

```go
// Correct: one welcome email per user, ever.
runlater.ID("welcome-email:" + userID)

// Wrong: the second call is silently deduplicated, not scheduled.
runlater.ID("daily-digest:" + userID)
```

Recurring work needs an ID that varies per occurrence (`"daily-digest:"+userID+":"+date`), or no ID at all.

Because a deduplicated handoff is reported as success, check the receipt when the distinction matters:

```go
receipt, err := later.Do(ctx, "email.send", payload, runlater.ID(key))
if err != nil {
    return err
}
if receipt.Deduplicated {
    // Already accepted earlier, and possibly already executed.
}
```

Backends that cannot tell the two cases apart always leave `Deduplicated` false.

This protects against an important distributed-systems failure mode:

```text
client ---- create task ----> provider
                         task accepted
client <--- response lost ---- X

client cannot know whether the first handoff succeeded
```

A stable logical identity makes retrying the **handoff** safer.

It does not provide exactly-once **execution**. Providers such as Cloud Tasks may deliver a task more than once. **Handlers must be idempotent.**

## Versioned wire protocol

For HTTP-style delivery, `runlater` owns a small provider-independent envelope:

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

The envelope is intentionally versioned from the beginning. Provider request formats should not become the application's protocol, and producers and consumers should be able to evolve without being coupled to Cloud Tasks, SQS, or another backend.

## Receiving jobs

`httpjob` is the minimal receiving-side adapter for push/HTTP-style executors. It converts the wire envelope back into a typed Go handler using only `net/http`.

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

Malformed envelopes and unknown jobs return 4xx responses. Handler failures return 5xx so the execution provider can apply its retry policy.

### Permanent failures

Retry policy belongs to the provider, but only the handler knows when retrying is pointless. A payload that cannot decode will not decode on the fourth attempt either.

```go
return fmt.Errorf("unknown account %q: %w", id, httpjob.ErrPermanent)
```

An error wrapping `httpjob.ErrPermanent` answers 200, which is the only response that stops a push-style provider — Cloud Tasks retries every non-2xx code, 4xx included. `HandleJSON` wraps its own decode failures this way.

### Observing failures

The HTTP response deliberately carries no detail, so set an error handler or job failures are invisible:

```go
mux.ErrorHandler = func(r *http.Request, env runlater.Envelope, err error) {
    log.Printf("job %s/%s failed: %v", env.Name, env.ID, err)
}
```

It sees handler errors and pre-handler rejections alike. Suppressing a retry never suppresses the report.

### Authentication

`httpjob` authenticates nothing. Anything that can reach the endpoint can enqueue work into your application, so the route must be protected by the deployment: Cloud Run IAM plus task-level OIDC, an internal-only ingress, a gateway, or equivalent. Keeping IAM at the edge is deliberate — it is only safe if the edge exists.

`httpjob` is intentionally small. It should not grow into a worker framework, scheduler, middleware ecosystem, or workflow engine.

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

On Cloud Run, the backend uses the metadata server for the attached service account's OAuth access token by default.

Authenticated targets can use task-level OIDC configuration. When possible, queue-level target/auth configuration is preferable because it keeps IAM and deployment policy outside application code.

### Delayed execution

```go
_, err := later.Do(ctx, "email.send", payload, runlater.After(5*time.Minute))
```

or:

```go
_, err := later.Do(ctx, "email.send", payload, runlater.At(runAt))
```

Provider quotas and evolving service limits are intentionally left to the provider API instead of being copied into `runlater` as policy that can become stale.

### Handoff failures

A failed enqueue returns `*cloudtasks.APIError`, so callers can separate a handoff worth retrying from one that never will be:

```go
var apiErr *cloudtasks.APIError
if errors.As(err, &apiErr) && apiErr.Retryable() {
    // 429 or 5xx. With a stable ID, a retry that arrives after the first
    // attempt actually landed is deduplicated rather than duplicated.
}
```

## Testing

The handoff boundary should be easy to test without reproducing production infrastructure.

```go
mem := &memory.Dispatcher{}
later := runlater.New(mem)

_, _ = later.Do(ctx, "email.send", payload, runlater.ID("test-job"))

jobs := mem.Jobs()
```

No emulator, credentials, Redis, database, or worker process is required for unit tests.

The in-memory backend accepts every handoff by default. To assert the property that actually breaks in production — a repeated `(Name, ID)` that the provider refuses — turn on deduplication:

```go
mem := &memory.Dispatcher{Deduplicate: true}
```

A repeat is then not recorded, and its receipt reports `Deduplicated`.

## Dependency policy

The module currently has **zero third-party dependencies**.

That is a product constraint, not a benchmark trick. The original motivation for `runlater` was that a tiny background handoff should not introduce gRPC, protobuf, a large cloud SDK dependency graph, unrelated vulnerability alerts, or another worker runtime.

Backends should remain standard-library-first where doing so does not compromise correctness. A dependency may be added if correctness clearly requires it; dependency count is not more important than semantics.

## What runlater will not become

`runlater` does **not** aim to become:

- a Redis/Postgres job server
- a worker-pool runtime
- a workflow or DAG engine
- a cron system
- a dashboard
- a generic message-broker abstraction
- a lowest-common-denominator API over every queue product

If an addition requires `runlater` to reimplement a capability that the execution provider already owns well, the default answer should be **no**.

## Backend acceptance criteria

A new backend should not be added merely because it can transport bytes.

A backend belongs in `runlater` only when the mapping preserves the handoff model without hiding correctness-relevant differences. In practice, it should satisfy most of the following:

1. **Durable acceptance** — success means responsibility has moved out of the request process.
2. **Provider-managed execution** — `runlater` does not need to introduce its own long-running worker runtime.
3. **Retryable delivery** — failed execution can be retried by the provider or its managed integration.
4. **Stable identity mapping** — `(Name, ID)` can map meaningfully to provider identity or deduplication semantics.
5. **Delayed execution when exposed** — `RunAt` is either supported honestly or rejected explicitly.
6. **Small adapter surface** — the backend does not force provider-specific types into application code.

This means "supports queues" is not enough. For example, a backend that requires `runlater` itself to operate polling workers would change the architecture and should probably live in a different project.

## Design principles

These principles are intended to constrain future development:

**1. Own the seam, not the system.**  
`runlater` owns the application boundary between "do this later" and a provider that already knows how to execute it.

**2. Thin API, strong semantics.**  
A small wrapper is valuable only if it removes accidental complexity while preserving correctness-relevant behavior.

**3. Provider differences are not bugs to abstract away.**  
Do not manufacture a fake universal queue model. Document backend guarantees and expose incompatibility when semantics do not map.

**4. Make ambiguous failure safer.**  
Logical IDs, receipts, and backend identity mapping should help callers reason about uncertain handoff outcomes.

**5. Keep infrastructure policy at the edge.**  
IAM, retry policy, queue rate limits, quotas, and deployment configuration belong to providers and infrastructure tooling unless application correctness requires otherwise.

**6. No worker infrastructure by accident.**  
Adding a backend must not quietly turn `runlater` into the job system it was created to avoid.

**7. Prefer boring Go.**  
Small interfaces, `context.Context`, `net/http`, `encoding/json`, explicit errors, and minimal dependencies are features.

## How it differs from existing Go job libraries

Traditional Go job frameworks usually own more of the system. That is useful when you want the application to own job persistence and worker execution.

`runlater` deliberately owns less:

| Concern | runlater | Traditional job framework |
| --- | --- | --- |
| Durable store | Provider owns it | Library/app usually owns it |
| Worker runtime | Provider-managed | Library/app-managed |
| Redis/Postgres required | No | Often |
| Provider SDK types in business code | No | N/A |
| Retry/rate limiting | Reuse provider | Framework config/runtime |
| Main abstraction | Handoff | Queue + workers |
| Workflow features | Non-goal | Often available |
| Third-party dependencies | 0 today | Usually several |

The goal is not to be more capable than those libraries. The goal is to make a much smaller architectural choice possible.

## Status

Pre-v1. The API and semantics are intentionally being challenged before the first stable release.

## License

MIT
