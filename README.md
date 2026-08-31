# s3-lease

`s3-lease` coordinates distributed workloads through one authoritative S3
object. It provides the backend-neutral lease core, the AWS SDK for Go v2
adapter, a scoped distributed-mutex recipe, and a single-use leader-election
recipe with blocking acquisition and automatic renewal.

The protocol uses conditional S3 writes and opaque ETags. A confirmed lease's
epoch is already persisted in the lease object. No client-local token store is
required. When leader work mutates a different protected resource, that
resource (or its authoritative S3 manifest) must also retain and enforce the
non-decreasing epoch atomically; holding a lease alone does not make external
writes safe.

Design references:

- [High-level design](docs/s3_lease_high_level_design.md)
- [Kubernetes Lease behavior comparison](docs/s3_lease_and_k8s_lease.md)

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

## Run a leader election

```go
elector, err := leaderelection.New(leaderelection.Config{
    Client:          client,
    RetryPeriod:     3 * time.Second,
    ObserveInterval: 2 * time.Second,
    ShutdownTimeout: 5 * time.Second,
    ReleaseOnCancel: false,
    Callbacks: leaderelection.Callbacks{
        OnStartedLeading: func(workCtx context.Context, epochID uint64) error {
            // Activate epochID at each protected resource before ordinary work.
            return runLeaderWork(workCtx, epochID)
        },
        OnStoppedLeading: func() {
            notifyLocalShutdown()
        },
        OnLeaderObserved: func(callbackCtx context.Context, observation lease.Observation) {
            reportSnapshot(observation) // Informational only; never grants authority.
        },
    },
})
if err != nil {
    return err
}
return elector.Run(ctx)
```

An `Elector` is single-use. Leader work starts only after a confirmed grant is
rechecked, receives the epoch as its fencing token, and is canceled and joined
before release. Normal work return always attempts a conditional release;
`ReleaseOnCancel` controls only caller cancellation. Work return ends the run;
the elector never reacquires. `OnStoppedLeading` is dispatched asynchronously
once stopping begins, and only when leader work was admitted; it is not a work
completion barrier. Observation callbacks are serial and coalesced polling
snapshots, not readiness, authority, or complete transition signals.

## Fence an S3 manifest

The reference manifest stores its watermark in S3, not on the election client:

```go
manifest, err := fencedmanifest.NewWriter(store, lease.Key{
    Bucket: "coordination", ObjectKey: "manifests/current.json",
})
if err != nil {
    return err
}

// Run this before the new leader reports business readiness.
if _, err := manifest.Activate(workCtx, epochID, activationID); err != nil {
    return err
}

// Every protected publication verifies the activated epoch in the same CAS.
_, err = manifest.Publish(workCtx, epochID, mutationID, payloadJSON)
return err
```

Activation is one conditional manifest write per leadership term. Subsequent
publications require the same epoch. For large data, first upload an immutable
object at a unique key, then publish its key/version/checksum in the small JSON
manifest. A higher lease epoch does not fence a different S3 key until that
manifest activation succeeds.

The API is a semantic analogue of classic Kubernetes client-go election, not a
drop-in implementation. In particular, a same-ID restart must acquire a higher
epoch, leader work is tracked and joined, and `OnStoppedLeading` is called only
after work actually started. APIs such as `GetLeader`, `IsLeader`, health
watchdogs, resource-lock variants, and coordinated election are intentionally
not provided.

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
contracts plus election and fencing faults, and removes the disposable Compose
volume even when a test fails:

```sh
make e2e
```

Override the fixture or published port when needed:

```sh
SEAWEEDFS_IMAGE=chrislusf/seaweedfs:4.44 \
S3_LEASE_E2E_PORT=18333 \
make e2e E2E_PROJECT=s3-lease-e2e-dev
```

Real AWS qualification is a separate release gate and requires a dedicated
Versioned bucket whose policy denies deletion and unconditional writes:

```sh
AWS_REGION=us-east-1 \
S3_LEASE_AWS_BUCKET=lease-qualification \
S3_LEASE_AWS_PREFIX=qualification/manual \
make test-aws
```

Passing SeaweedFS E2E does not by itself qualify AWS IAM, encryption,
Versioning, signing, or exact service error behavior.

Build the current-platform candidate image with `make docker-build`. Publishing
a multi-architecture manifest requires a registry-qualified image and registry
credentials:

```sh
make docker-buildx \
  IMG=registry.example.com/team/s3-lease-e2e:v0.1.0 \
  PLATFORMS=linux/amd64,linux/arm64
```
