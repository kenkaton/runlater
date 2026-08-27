# runlater

> Run work later. Reliably.

`runlater` is a tiny durable-background-work primitive for Go.

Application code says **what should run later**. A dispatcher decides **how it is durably delivered**.

```go
later := runlater.New(dispatcher)

err := later.Do(ctx, "email.send", EmailPayload{
    UserID: 42,
})
```

The first production dispatcher uses Google Cloud Tasks over its REST API — no gRPC, no protobuf, and no Google Cloud Go SDK.

## Why

A small piece of background work should not force cloud-specific task types through your application or pull a large SDK dependency graph into your service.

`runlater` keeps the application boundary deliberately small:

```go
Do(ctx, name, payload, ...options)
```

Its intended semantics are:

- durable handoff to the configured backend
- at-least-once execution (when provided by the backend)
- JSON-serializable payloads
- optional delayed execution
- provider-specific details stay at the edge

`runlater` is not trying to hide meaningful differences between every queue system. It defines a small background-work primitive and lets backends implement that primitive honestly.

## Install

```bash
go get github.com/kenkaton/runlater
```

## Google Cloud Tasks

```go
package main

import (
    "context"

    "github.com/kenkaton/runlater"
    "github.com/kenkaton/runlater/cloudtasks"
)

type EmailPayload struct {
    UserID int64 `json:"user_id"`
}

func enqueue(ctx context.Context) error {
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
    return later.Do(ctx, "email.send", EmailPayload{UserID: 42})
}
```

On Cloud Run, `cloudtasks.New` uses the metadata server for the attached service account's OAuth access token by default.

The target receives:

```json
{
  "name": "email.send",
  "payload": {
    "user_id": 42
  }
}
```

### Delayed work

```go
err := later.Do(ctx, "email.send", payload, runlater.After(5*time.Minute))
```

or at a specific time:

```go
err := later.Do(ctx, "email.send", payload, runlater.At(runAt))
```

### Authenticated Cloud Run targets

If authentication is configured per task:

```go
dispatcher, err := cloudtasks.New(cloudtasks.Config{
    Project:             "my-project",
    Location:            "asia-northeast1",
    Queue:               "default",
    TargetURL:           "https://my-service.run.app/internal/jobs",
    ServiceAccountEmail: "cloud-tasks@my-project.iam.gserviceaccount.com",
})
```

You can also configure OIDC at the Cloud Tasks queue level and omit `ServiceAccountEmail` from application code.

## Testing

Use the in-memory dispatcher without Cloud Tasks, credentials, an emulator, or a worker process:

```go
mem := &memory.Dispatcher{}
later := runlater.New(mem)

_ = later.Do(ctx, "email.send", payload)

jobs := mem.Jobs()
```

## Dependency policy

The module currently has **zero third-party dependencies**.

The Cloud Tasks backend talks directly to the documented REST API using `net/http`. This is intentional: keeping the dependency and security-alert surface small is one of the project's core goals.

## Scope

The initial focus is Cloud Run + Cloud Tasks because that is where the original problem showed up. The core API is provider-neutral so other serverless-native durable backends can be added later without turning the project into a generic queue framework.

Potential future backends include SQS and local/inline execution, but only where their semantics map cleanly to the primitive.

## Status

Early development. Expect API changes before v1.

## License

MIT
