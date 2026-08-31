# S3 Lease and Kubernetes Lease Behavior

This document compares this repository with Kubernetes `Lease` objects and the
classic `client-go/tools/leaderelection` implementation. It explains where the
mental models align and where application behavior must differ.

For the normative S3 protocol, see
[S3 Lease: High-Level Design](s3_lease_high_level_design.md).

## 1. Executive comparison

Both systems use optimistic concurrency on one durable record, locally observe
an unchanged record before takeover, and expose a holder identity for
diagnosis. They are semantic relatives, not wire-compatible implementations.

The important differences are lifecycle and fencing:

- Kubernetes client-go starts leader work without joining it; this repository
  tracks, cancels, and joins the work before release.
- Kubernetes client-go invokes `OnStoppedLeading` synchronously as `Run`
  exits, including exits before acquisition. This repository dispatches it
  asynchronously and only after leader work was admitted.
- A restarted client-go participant with the same identity may continue the
  existing record. This repository requires every new acquisition to advance
  the epoch.
- Neither a Kubernetes Lease nor an S3 lease can by itself stop a stale process
  from writing elsewhere. This repository makes the resource-fencing contract
  explicit and passes the epoch directly to protected work.

## 2. Concept mapping

| Kubernetes | S3 lease | Important distinction |
|---|---|---|
| API server and storage | One qualified S3-compatible backend | The deployment must prove the required S3 conditional-write semantics. |
| `coordination.k8s.io/v1 Lease` | JSON lease record | The schemas are intentionally different. |
| Object namespace/name | Bucket and full object key | The key is immutable for the coordination lifetime. |
| Object UID | `metadata.uid` | Both detect replacement; the S3 object must never be recreated. |
| `metadata.resourceVersion` | Opaque ETag/`Version` | Both are equality preconditions, not ordered fencing tokens. |
| `holderIdentity` | `clientID` | Both identify a participant; neither proves a live process. |
| `leaseDurationSeconds` | `leaseDurationSeconds` | Both describe the observation window before takeover. |
| `acquireTime`, `renewTime` | Same diagnostic fields | The S3 protocol never uses wall-clock comparison for expiry. |
| `leaseTransitions` | `epochID` | The S3 epoch advances on every acquisition and is exported for fencing. |
| Resource update count | `sequenceID` | The S3 sequence distinguishes renew/release mutations within an epoch. |

## 3. Storage concurrency

Kubernetes updates a Lease through the API server using resource-version
optimistic concurrency. The API server supplies validation, admission,
authorization, persistence, and a watchable object model.

This repository narrows the storage requirement to strong reads plus two
conditional writes:

```text
create only if absent
replace only if current ETag equals expected ETag
```

That narrower interface keeps AWS types out of the core, but shifts deployment
responsibility to backend qualification and bucket policy. Passing a local S3
emulator does not prove the configuration of a real AWS bucket.

## 4. Acquisition and expiry

### 4.1 Shared behavior

Classic client-go leader election does not decide expiry by trusting the
holder's wall-clock `renewTime`. It remembers when the observed record last
changed locally and treats it as expired only after a lease duration without
change. The S3 protocol follows the same conservative idea.

This protects both designs from ordinary clock skew between participants. A new
observer may wait longer than the nominal expiry interval, but it does not take
over merely because another host's timestamp appears old.

### 4.2 Different identity rules

Client-go has special handling when the stored holder identity equals its own
configured identity. This supports continued renewal by the same logical
participant, but identity reuse can blur a process restart or overlap.

The S3 protocol treats `clientID` as a label only. Every acquisition after an
earlier grant, including one by the same ID, uses conditional replacement and
advances `epochID`. This creates an unambiguous fencing generation for the new
process.

### 4.3 Blocking behavior

The S3 core's `Require` makes one bounded attempt and returns contention.
Leader-election and mutex recipes own the wait/retry loop. This separation keeps
storage transitions independent of scheduling policy.

Client-go exposes the retrying election lifecycle as its primary abstraction.
The difference is API shape rather than a different conditional-write safety
principle.

## 5. Leader work lifecycle

### 5.1 Start callback

In client-go, `OnStartedLeading` is launched in a goroutine. Its return value is
not used to stop election, and `Run` does not join that goroutine. Applications
therefore own the callback's child-work and shutdown discipline.

Here, `OnStartedLeading` is the protected work. It also runs in a goroutine, but
the recipe tracks it. Its return ends the election run, its context is canceled
on leadership loss, and release is attempted only after it has joined.

This stricter contract prevents a recipe from advertising release while its own
known work is still running.

### 5.2 Stop callback

The two APIs give similarly named callbacks different lifecycle roles:

| Question | Kubernetes client-go | This repository |
|---|---|---|
| When is it invoked? | Deferred when `Run` exits, even if acquisition never succeeded. | Only if leader work was actually admitted. |
| Synchronous with `Run`? | Yes; `Run` waits for it to return. | No; it is dispatched asynchronously. |
| Does it stop or join leader work? | No. | No. Tracked work is stopped and joined separately. |
| May it block cleanup? | Yes. | No. |
| Intended role here | Application-defined notification. | Best-effort local notification only. |

For this repository, required cleanup must stay inside `OnStartedLeading` and
complete before it returns. `OnStoppedLeading` is suitable for signals such as
metrics, logging, or waking a local supervisor. Making it asynchronous keeps an
unrelated notification from delaying cancellation and lease cleanup.

### 5.3 Release behavior

Client-go's `ReleaseOnCancel` optionally releases after its renewal loop ends.
Because leader work is not joined by the library, its documentation requires
the application to ensure work has stopped before enabling prompt release.

This repository always attempts release after normal tracked-work return.
`ReleaseOnCancel` applies only when the caller cancels `Run`. On cancellation or
leadership loss, the recipe cancels and joins work first. If work does not stop
within `ShutdownTimeout`, release is suppressed and `ErrWorkNotStopped` is
returned.

## 6. Observation behavior

Kubernetes offers a general watchable API, but classic client-go leader
election repeatedly reads and updates its resource lock. Its callbacks do not
constitute a complete leadership event stream.

The S3 recipe polls one object. `OnLeaderObserved` receives successful snapshots
serially, with coalescing when the consumer is slow. Delivery is informational:
it does not grant authority, prove readiness, or guarantee that every transition
was observed.

Applications needing a durable audit log or complete transition feed should
build that as a separate facility rather than infer it from polling callbacks.

## 7. Fencing and stale leaders

Kubernetes client-go explicitly does not guarantee that only one participant is
acting as leader. A long pause or network partition can leave an old process
running after another participant acquires the Lease. Kubernetes controllers
usually rely on idempotent reconciliation, API resource versions, or
domain-specific validation to limit harm.

The same physical limitation applies to S3. Context cancellation is
cooperative and cannot revoke an external write already in flight.

This repository therefore gives each acquisition a monotonic `epochID` and
requires protected resources to retain the highest activated epoch. A write is
safe only when the resource atomically validates that epoch. The ETag serializes
lease-record mutations; the epoch fences application-resource mutations. They
solve different problems and are not interchangeable.

## 8. Failure and uncertainty

The Kubernetes API presents a structured server boundary, while an S3 client
can lose a write response after the service committed the object. Retrying with
a newly generated record could skip state or create false authority.

The S3 core therefore never converts an ambiguous acquisition into a grant. For
an ambiguous renewal or release within an existing grant, it freezes the exact
expected ETag and bytes and reconciles them under the original deadline. This
behavior is more visible than typical client-go handling because S3 write
ambiguity is part of this protocol's public safety contract.

## 9. Which model should an application use?

Use the Kubernetes model when the workload already runs against a Kubernetes
API server and its operational assumptions fit the application. It benefits
from the platform's established API machinery and ecosystem.

Use this S3 model when S3 is the shared durable authority, the exact backend has
passed conditional-write qualification, and protected resources can enforce
epochs. Do not choose it merely because all participants can read a bucket.

When migrating code from client-go, review these points explicitly:

- leader work must return and is joined;
- normal work return ends the election;
- every acquisition receives a new epoch, even with the same client ID;
- `OnStoppedLeading` is asynchronous and only follows admitted work;
- observation callbacks are coalesced snapshots; and
- external writes must enforce the supplied epoch.

## 10. Primary references

- [Kubernetes Lease API](https://kubernetes.io/docs/reference/kubernetes-api/coordination/lease-v1/)
- [Kubernetes Leases concept](https://kubernetes.io/docs/concepts/architecture/leases/)
- [client-go leader election source](https://github.com/kubernetes/client-go/blob/master/tools/leaderelection/leaderelection.go)
- [client-go leader election package](https://pkg.go.dev/k8s.io/client-go/tools/leaderelection)
- [Amazon S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
