# s3-lease

`s3-lease` coordinates distributed workloads through one authoritative S3
object. It provides the backend-neutral lease core, the AWS SDK for Go v2
adapter, and a scoped distributed-mutex recipe with blocking acquisition and
automatic renewal. Leader-election behavior is still planned.

The protocol uses conditional S3 writes and opaque ETags. A confirmed lease's
epoch is a fencing token, but the application must persist and enforce that
token atomically at every protected resource; holding a lease alone does not
make external writes safe.

## Construct a client and acquire a lease

```go
logger := common.InitLogger(false) // Shared development logger.

store, err := s3store.New(s3store.Config{
    Client: awsS3Client,
    Logger: logger, // Optional; defaults to the shared logger.
})
if err != nil {
    return err
}

client, err := lease.New(lease.Config{
    Store:          store,
    Key:            lease.Key{Bucket: "coordination", ObjectKey: "leases/worker.json"},
    ClientID:       "worker-a",
    MetadataName:   "worker",
    LeaseDuration:  30 * time.Second,
    RenewDeadline:  20 * time.Second,
    RequestTimeout: 2 * time.Second,
    Logger:         logger, // Optional; defaults to the shared logger.
})
if err != nil {
    return err
}

acquired, err := client.Require(ctx)
if err != nil {
    return err
}
if err := acquired.Check(); err != nil {
    return err
}
epoch := acquired.EpochID()
_ = epoch // Activate and enforce this token at the protected resource.
```

`Require` makes one bounded acquisition attempt; it does not wait in a retry
loop. Direct users own renewal scheduling, work cancellation and joining, and
optional release. `Done` closes on expiry or retirement, and no later storage
response can revive that lease.

## Run protected work with the mutex recipe

```go
lock, err := mutex.New(mutex.Config{
    Client:          client,
    RetryPeriod:     3 * time.Second,
    ObserveInterval: 2 * time.Second,
    ShutdownTimeout: 5 * time.Second,
    ReleaseOnCancel: false,
    Logger:          logger,
})
if err != nil {
    return err
}

err = lock.WithLock(ctx, func(workCtx context.Context, epochID uint64) error {
    // Activate and enforce epochID at the protected resource before writes.
    return runProtectedWork(workCtx, epochID)
})
```

`WithLock` retries until it confirms a grant, renews while the tracked work is
running, and waits for that work to return before release. Normal completion
always attempts a conditional release. Cancellation stops renewal immediately;
with the default `ReleaseOnCancel: false`, the stored owner remains occupied
until another participant observes that version unchanged for its advertised
lease duration and wins a conditional takeover. Expiry does not write an
unlocked record.

One `Mutex` permits one active invocation and has no local FIFO queue. It can
be reused after its prior work has joined and the prior grant has released or
retired. `ErrWorkNotStopped` means work ignored cancellation, release was
suppressed, and that `Mutex` must not be reused.

The `Work` callback lets `WithLock` enforce cancellation and join-before-release
without knowing how the application starts child tasks. Its context closes on
caller cancellation or lease loss, its epoch argument is the resource fencing
token, and its returned error is propagated from `WithLock`.

Callers that own their lifecycle explicitly can instead use the non-blocking
manual API:

```go
held, err := lock.TryLock(ctx) // Exactly one acquisition attempt.
if err != nil {
    return err
}
defer func() { _ = lock.Release(context.Background(), held) }()

epoch := held.EpochID()
select {
case <-held.Done():
    return held.Check()
default:
    return writeWithFence(epoch)
}
```

A successful `TryLock` starts automatic renewal. Its call context bounds only
the acquisition attempt; ownership continues until loss or `Release`. The
caller must stop and join its own protected work before calling `Release`.
Passing the acquisition-scoped `*mutex.Lock` prevents a delayed release from
retiring a newer acquisition. Contention returns the core acquisition error
immediately; `TryLock` never enters the `WithLock` retry loop.

`common.InitLogger(true)` installs one process-wide production Zap logger using
JSON at Info level. Passing `false` installs the console development logger at
Debug level. Components use the shared logger when their `Logger` field is nil.

## Build and test

```sh
make help
make build
make vet
make test-race
```

Local E2E uses Docker/Docker Compose only. It starts pinned SeaweedFS 4.44,
waits for an S3 create/put/get readiness round trip, builds the test candidate
image, runs the conditional-write, lease-core, and distributed-mutex
contracts, and removes the disposable Compose volume even when a test fails:

```sh
make e2e
```

Override the fixture or published port when needed:

```sh
SEAWEEDFS_IMAGE=chrislusf/seaweedfs:4.44 \
S3_LEASE_E2E_PORT=18333 \
make e2e E2E_PROJECT=s3-lease-e2e-dev
```

Build the current-platform candidate image with `make docker-build`. Publishing
a multi-architecture manifest requires a registry-qualified image and registry
credentials:

```sh
make docker-buildx \
  IMG=registry.example.com/team/s3-lease-e2e:v0.1.0 \
  PLATFORMS=linux/amd64,linux/arm64
```

See [the high-level design](docs/s3_lease_high_level_design_en.md) for the
protocol and safety model, and
[implementation status](docs/implementation_status.md) for delivered and
deferred capabilities.
