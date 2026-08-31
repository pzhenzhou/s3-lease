# S3 Lease: High-Level Design

**Status:** protocol reference
**Scope:** lease core, leader-election recipe, mutex recipe, and resource fencing

This document defines the safety model and the reasons behind the main design
decisions. It intentionally leaves implementation mechanics to the Go packages
and their tests. For a behavior-by-behavior comparison with Kubernetes, see
[S3 Lease and Kubernetes Lease Behavior](s3_lease_and_k8s_lease.md).

The words **must**, **must not**, **should**, and **may** are normative.

## 1. Design intent

The system coordinates independent processes through one durable S3 object.
S3 is useful here because conditional object writes can provide the single
serialization point required by a lease without adding a separate coordination
service.

The design follows five principles:

1. **Safety before availability.** Uncertainty never grants authority. Within a
   confirmed grant, an unresolved mutation blocks new mutations until its exact
   outcome is reconciled or local authority expires.
2. **Storage grants authority; local state does not.** A process becomes a
   holder only after a conditional write is confirmed in time.
3. **Time is observed locally.** Persisted wall-clock timestamps aid diagnosis;
   they are not trusted for expiry decisions.
4. **Lease ownership and write safety are different problems.** The lease elects
   a participant. Each protected resource must reject stale epochs.
5. **Lifecycle ownership must be explicit.** Recipes start, cancel, and join the
   work they protect. Notifications do not own that work.

These choices deliberately reject convenient but unsafe behavior such as
assuming a timed-out write failed, reviving an expired local handle, or treating
a holder identity as proof that a restarted process still owns a lease.

## 2. Scope and boundaries

The repository has three distinct layers:

```text
application resources  <- validate fencing epochs
        recipes         <- own work lifecycle and renewal policy
      lease core        <- define authority, transitions, and uncertainty
     LeaseStore         <- translate conditional object operations
          S3            <- serialize one object key
```

- `lease/` is backend-neutral. It owns record validation, acquisition, renewal,
  release, expiry, and reconciliation.
- `lease/s3store/` translates S3 responses and opaque ETags into the narrow
  `LeaseStore` contract. It contains no election policy.
- `recipes/leaderelection/` and `recipes/mutex/` add retry loops, renewal
  scheduling, cancellation, and work joining.
- Applications own resource-specific fencing. The example in
  `examples/fencedmanifest/` shows one S3 implementation.

This is a cooperative lease protocol, not consensus, a transaction service, a
fair queue, or a cross-resource lock. It assumes one authoritative backend,
bucket, Region, and full object key for the coordination lifetime.

## 3. Storage and identity model

### 3.1 Required storage contract

The core needs only three operations:

```go
type LeaseStore interface {
    Get(context.Context, Key) (StoredObject, error)
    CreateIfAbsent(context.Context, Key, []byte) (Version, error)
    CompareAndSwap(context.Context, Key, Version, []byte) (Version, error)
}
```

`CreateIfAbsent` must be atomic. `CompareAndSwap` must replace the current body
only when the supplied version is current. `Get` after a successful write must
return that body and version. A `Version` is an opaque equality token; clients
must never parse or order it.

The lease object must not be deleted, recreated, restored to an older version,
or copied between keys. Keeping one stable object preserves its UID and the
monotonic history on which rollback detection and fencing depend.

### 3.2 Persisted record

The JSON record contains only protocol state and diagnostic metadata:

| Field | Meaning |
|---|---|
| `metadata.uid` | Stable identity of the lease object. |
| `clientID` | Current holder label; useful for diagnosis, never proof of process identity. |
| `leaseDurationSeconds` | Duration another participant must observe an occupied record unchanged before takeover. |
| `epochID` | Monotonic fencing generation; advances on every acquisition. |
| `sequenceID` | Mutation sequence within an epoch; advances on renewal and release. |
| `acquireTime`, `renewTime` | Human-readable timestamps; not the expiry clock. |

The initial acquisition creates epoch 1, sequence 1. A takeover increments the
epoch and resets the sequence to 1. Renewal and release keep the epoch and
increment the sequence. Release clears `clientID`; it does not delete the
object or reset its history.

`clientID` is intentionally not a session token. A process using the same ID as
an earlier process must still win a new conditional acquisition and receive a
higher epoch.

## 4. Authority protocol

### 4.1 One acquisition attempt

`Client.Require` performs one bounded attempt:

1. Read the current object.
2. If it is missing, propose an atomic create.
3. If it is explicitly released, or has been observed unchanged for its full
   advertised duration, propose a conditional takeover.
4. Return a lease handle only after the write succeeds before the proposal's
   fixed authority deadline.

Contention is an ordinary result. Waiting and retry policy belong to recipes,
not the core.

### 4.2 Local observation and expiry

Clients do not compare their wall clock with `renewTime`. On the first view of a
record version, the client starts a local monotonic observation interval. Any
body or version change restarts that interval. Only an unchanged occupied
record observed for its full lease duration becomes eligible for takeover.

This avoids requiring synchronized clocks. It also means a new observer waits a
full duration even if the former holder appears old, favoring safety after
restarts, pauses, and clock jumps.

A confirmed local lease has its own deadline derived from the proposal's first
send time. Once expired or retired, the handle closes permanently; a delayed
response cannot revive it.

### 4.3 Renewal and release

Renewal conditionally writes the next sequence while the exact preceding
version is current. A conflict means the caller can no longer establish that it
owns the current record.

Release first retires local authority, then conditionally writes an empty
holder at the next sequence. Local retirement is irreversible even if the
release response is lost. Release is an availability optimization, not a
safety requirement: if it is omitted, another participant can take over only
after observing the occupied record unchanged for a full duration.

### 4.4 Unknown write outcomes

A timeout, cancellation, connection loss, or missing response version can make
a mutation ambiguous. The client cannot safely treat it as either committed or
failed.

An ambiguous acquisition returns `ErrUnknownOutcome` without a lease handle.
Even an exact readback match cannot manufacture local authority, because a
different process may have proposed identical contents. The abandoned write may
leave an occupied record until ordinary expiry; that availability cost is
intentional.

For renewal or release within a confirmed grant, the core freezes:

- the exact body,
- the expected storage version,
- the mutation kind, and
- the original first-send deadline.

Later `Renew` or `Release` calls may only read and reconcile that proposal or
resend its exact bytes. They must not allocate another sequence or extend its
deadline. A different proposal is blocked until the outcome is known or local
authority is retired. This is the central safety-over-availability rule.

## 5. Recipe lifecycle

The core exposes mechanisms; recipes define the common safe lifecycle.

### 5.1 Shared holder behavior

After a confirmed grant, a recipe:

1. rechecks the lease before admitting work;
2. starts one tracked work function with a cancelable context and epoch;
3. renews while that work is active;
4. cancels work on caller cancellation, lease loss, or a fatal observer error;
5. waits for work to return before releasing; and
6. suppresses release if work misses `ShutdownTimeout`.

Normal work return always requests release. `ReleaseOnCancel` controls only
caller cancellation. It defaults to false because immediate release is unsafe
when untracked external activity might survive cancellation.

`ErrWorkNotStopped` means the work ignored cancellation. Authority is not
released, and that recipe instance must not admit new protected work.

### 5.2 Leader election

An `Elector` is single-use. It waits for one confirmed acquisition, runs
`OnStartedLeading`, and ends when that work returns or leadership is lost. It
does not reacquire after work has started.

Callback roles are intentionally different:

| Callback | Execution and meaning |
|---|---|
| `OnStartedLeading` | Runs as tracked work in its own goroutine. Its return value ends the leadership lifecycle. It is canceled and joined. |
| `OnStoppedLeading` | Dispatched asynchronously, at most once, only after work was admitted. It is an informational notification, not a cleanup barrier. |
| `OnLeaderObserved` | Dispatched asynchronously, serially, and with coalescing. It reports snapshots only; it never grants authority or readiness. |

The stop callback is asynchronous so a slow notification cannot delay work
cancellation, joining, or lease cleanup. Code that must complete before release
belongs in `OnStartedLeading` and must finish before that function returns.

### 5.3 Mutex

`WithLock` applies the same tracked lifecycle to a reusable, sequential mutex.
`TryLock` performs one acquisition attempt and returns a handle for callers that
explicitly own renewal and release sequencing. There is no local FIFO queue and
no fairness guarantee.

## 6. Fencing protected resources

Cancellation cannot stop a paused process, revoke a request already in flight,
or prevent an old leader from resuming. Therefore the lease epoch is a fencing
token, not merely metadata.

Before doing ordinary work, a new holder must activate its epoch at every
protected resource. Each mutation must atomically enforce:

```text
request epoch == resource accepted epoch
```

Activation may advance the accepted epoch but must never move it backward.
After epoch 12 is activated, a write from epoch 11 must fail even if that process
still believes it is leader.

The fenced-manifest example stores an accepted epoch and application payload in
one conditionally replaced S3 object. It also retains a small bounded mutation
history so a response-lost write can be recognized after intervening commits.
Large data should be written immutably first and then referenced by the fenced
manifest.

If a resource cannot atomically persist and validate a fencing token, this
system provides cooperative election for that resource, not stale-writer
safety.

## 7. Timing choices

Configuration must satisfy:

```text
LeaseDuration > RenewDeadline > RetryPeriod > 0
RequestTimeout > 0
ShutdownTimeout > 0
```

There must be meaningful margin between retry and renewal deadlines. Values
should be derived from measured request latency, scheduler pauses, retry budget,
and the application's shutdown time rather than copied from another system.

Longer durations reduce false takeovers but slow recovery. Shorter retry periods
improve renewal opportunities but increase S3 traffic. No timing choice removes
the need for fencing.

## 8. Failure policy

| Failure | Required behavior |
|---|---|
| Process crash or network partition | Stop renewing; takeover waits for a full unchanged observation duration. |
| Scheduler pause | Local authority expires independently; protected resources reject stale epochs. |
| Conditional-write conflict | Do not claim success; reread through a later attempt. |
| Ambiguous mutation response | Preserve and reconcile the exact proposal; do not advance protocol state. |
| Work ignores cancellation | Return `ErrWorkNotStopped` and do not release. |
| Object deletion, replacement, or rollback | Report a protocol violation; never silently reinitialize. |
| Malformed or incompatible record | Fail closed. |

These rules may temporarily reduce availability. That is preferable to issuing
two plausible grants or losing the ability to fence stale work.

## 9. Backend qualification and verification

A backend is supported only if tests qualify the exact service, endpoint
configuration, credentials, and SDK retry behavior used in production.
Automatic SDK retries must be disabled for the qualification rounds so one
contender means one mutation request.

The minimum conditional-write gate is:

- 100 fresh-key create races with at least 32 contenders;
- exactly one successful `CreateIfAbsent` per race;
- 100 fresh-key CAS races with at least 32 contenders sharing one expected
  version;
- exactly one successful CAS per race; and
- a final strong read whose body and version match the reported winner.

Protocol and recipe tests must also cover unknown outcomes, local expiry,
rollback, cancellation races, join timeouts, and stale-writer rejection. Timer,
lease, callback, and shared-state changes must pass `go test -race ./...`.

Local SeaweedFS tests are a development gate. Real AWS S3 qualification is a
separate release gate because IAM, signing, encryption, Versioning, and service
error behavior are part of the deployed contract.

## 10. Operational requirements

- Use one stable bucket, Region, endpoint, and full object key.
- Require TLS and encryption appropriate to the deployment.
- Grant only the read and conditional-write permissions the protocol needs.
- Deny deletion, unconditional overwrite, restoration, and lifecycle expiry of
  coordination objects.
- Enable Versioning for recovery and audit, but never use rollback as a normal
  protocol operation.
- Treat logs, metrics, observations, and wall-clock timestamps as diagnostics,
  not authority.
- Alert on protocol violations, counter rollback, unknown outcomes, renewal
  exhaustion, work shutdown timeouts, and fencing rejection.

## 11. Deliberate non-features

The design does not provide fairness, multi-object atomicity, reader/writer
locks, a complete transition stream, a health oracle, or automatic fencing of
arbitrary external systems. These would enlarge the interface without making
the core lease safer. They belong in separate modules only when a concrete use
case can define their correctness contract.

## 12. References

- [S3 Lease and Kubernetes Lease Behavior](s3_lease_and_k8s_lease.md)
- [Kubernetes Lease API](https://kubernetes.io/docs/reference/kubernetes-api/coordination/lease-v1/)
- [client-go leader election implementation](https://github.com/kubernetes/client-go/blob/master/tools/leaderelection/leaderelection.go)
- [Amazon S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)
- [Amazon S3 consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)
