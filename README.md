# s3-lease

`s3-lease` coordinates distributed workloads through one authoritative S3
object. The P0 implementation provides the backend-neutral lease core and the
AWS SDK for Go v2 adapter. Higher-level mutex and leader-election behavior is
still planned.

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
image, runs the conditional-write and lease-core contracts, and removes the
disposable Compose volume even when a test fails:

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
