# s3-lease

`s3-lease` is a Go library for coordinating distributed workloads through an
authoritative S3 object. It applies S3 conditional writes and strongly
consistent reads to a Kubernetes Lease-style protocol, avoiding the need for a
separate coordination service such as etcd or ZooKeeper.

The project is currently a design-first framework: the public boundaries and
protocol model are defined, while the core workflows and recipes are still
being implemented.

## Key features

- Lease acquisition, renewal, release, and observation through a small,
  backend-neutral core API.
- Conditional S3 writes using ETags for compare-and-swap coordination.
- Monotonic leadership epochs that applications can use as fencing tokens.
- Distributed mutex and leader-election recipes built on the lease core.
- An AWS SDK for Go v2 storage adapter boundary with backend-neutral error
  handling.
- Explicit module boundaries between storage, lease protocol, coordination
  recipes, and application-level resource fencing.

See [the high-level design](docs/s3_lease_high_level_design_en.md) for the
protocol, safety model, and architectural decisions.
