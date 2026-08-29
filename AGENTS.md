# Repository Guidelines

## Architecture and Priorities

Correctness and concurrency safety take priority over convenience and performance. The API is hierarchical: `lease/` provides the backend-neutral coordination core, while `recipes/leaderelection/` and `recipes/mutex/` build higher-level workflows from it. `s3store/` adapts AWS SDK types to the core, and `examples/fencedmanifest/` demonstrates application-level fencing. Treat `docs/s3_lease_high_level_design_en.md` as the safety and protocol reference.

Keep storage details out of the core and coordination policy out of storage adapters. A locally held lease is not sufficient protection for external writes; propagate and enforce fencing epochs where required.

## Project Structure

- `lease/`: records, errors, metrics, public contracts, and protocol state.
- `s3store/`: S3 conditional-write adapter.
- `recipes/`: leader-election and mutex workflows; shared private code lives in `recipes/internal/`.
- `examples/`: integration patterns.
- `docs/`: architecture, invariants, and planned E2E design.

The repository is currently design-first, so some types define planned behavior rather than complete implementations.

## Implementation Dependencies

Use these project-wide choices consistently during implementation:

- Concurrency: replace standard-library concurrency primitives with `github.com/puzpuzpuz/xsync/v4` equivalents. Install with `go get github.com/puzpuzpuz/xsync/v4`.
- JSON: use `github.com/goccy/go-json` instead of `encoding/json`; retain the familiar `json` package name.

  ```diff
  -import "encoding/json"
  +import "github.com/goccy/go-json"
  ```

- Collections: use `github.com/samber/lo` v1 and a functional collection-processing style. Install with `go get github.com/samber/lo@v1`.
- Retry: when exponential backoff is required, install `github.com/cenkalti/backoff/v7`. Classify permanent failures explicitly and bound every retry by context and attempt count:

  ```go
  result, err := backoff.Retry(ctx, func() (string, error) {
      resp, err := http.Get("https://www.example.com")
      if err != nil {
          return "", err
      }
      defer resp.Body.Close()
      if resp.StatusCode >= 500 {
          return "", fmt.Errorf("server error: %s", resp.Status)
      }
      if resp.StatusCode >= 400 {
          return "", backoff.Permanent(fmt.Errorf("client error: %s", resp.Status))
      }
      return "ok", nil
  }, backoff.WithMaxTries(5))
  ```

Do not add a dependency until code uses it. Afterward, run `go mod tidy` and review both module files.

## Module Boundaries and Interface Design

Dependencies between modules—including any future modules under `pkg/`—must cross interfaces so changes remain local. Define the interface in the consuming package. Prefer deep modules: a narrow, stable contract should hide validation, retry, serialization, storage, and synchronization details.

Existing core contracts illustrate the intended design:

```go
type LeaseStore interface {
    Get(context.Context, Key) (StoredObject, error)
    CreateIfAbsent(context.Context, Key, []byte) (Version, error)
    CompareAndSwap(context.Context, Key, Version, []byte) (Version, error)
}

type Clock interface {
    Now() time.Time
    AfterFunc(time.Duration, func()) (stop func() bool)
}
```

`s3store` satisfies `LeaseStore` without leaking AWS types into `lease`; a real or fake clock satisfies `Clock` without exposing timer implementation.

### Design and Refactoring Principles

Apply the principles from *A Philosophy of Software Design* during initial design and every refactor:

- **Manage complexity:** minimize change amplification, cognitive load, and hidden dependencies. Prefer designs where a contributor can change one concern without understanding unrelated implementation details.
- **Build deep modules:** expose a small, stable interface backed by substantial capability. Avoid shallow wrappers, pass-through methods, and interfaces that merely mirror an implementation.
- **Hide information:** keep volatile decisions—wire formats, ETag handling, retry policy, clocks, synchronization, and SDK behavior—inside their owning module. Do not expose internal state to save a small amount of code.
- **Keep abstractions distinct:** adjacent layers should offer different abstractions. The core defines lease semantics; adapters translate storage behavior; recipes define user workflows. Avoid duplicating the same policy across layers.
- **Design the common case:** make the safest operation simple, remove configuration that cannot be used safely, and define errors only where callers can respond meaningfully.
- **Prefer general mechanisms with specific entry points:** reusable machinery belongs in the core or internal helpers, while recipe APIs should remain task-focused.
- **Refactor toward clarity:** treat repeated pass-through code, temporal decomposition, information leakage, vague names, and coupled edits as design warnings. Preserve behavior with tests while moving knowledge to one authoritative module. Remove dead code as part of every refactor, bug fix, or feature change. Merge redundant behavior into one authoritative implementation even when the duplicated logic has different syntax or structure. Do not create additional interfaces, layers, or files unless they reduce overall complexity.

## Build and Verification

- `go build ./...`: compile all packages.
- `go test ./...`: run unit tests and compile checks.
- `go test -race ./...`: detect concurrency defects.
- `go vet ./...`: run standard static analysis.
- `gofmt -w <files>`: format changed Go sources.

The E2E commands and `test/e2e/` paths in the design document are proposals, not current runnable artifacts.

## Coding and Testing Style

Follow *Effective Go* and `gofmt`; use idiomatic names and doc comments for exported APIs. Comments should explain invariants, ownership, safety reasoning, and design tradeoffs—not restate code.

- Strive for a functional programming style: favor pure transformations, explicit inputs and outputs, immutable values, composition, and collection operators over hidden mutation and imperative loops. Isolate unavoidable state changes at clear ownership boundaries.
- Do not leave dead code, obsolete branches, unused abstractions, or redundant logic. Redundancy includes separate implementations of the same rule or policy, not only textually identical code; consolidate it without weakening module boundaries.
- Do not divide code too granularly. In particular, do not create one file per struct; group cohesive types, state, and behavior in the same file.
- Target approximately 500 lines or fewer per Go file, including test files. A modest excess is acceptable when it preserves cohesion, but files should not grow excessively.
- Order top-level declarations as: non-struct types and interfaces, constants, variables, structs, then constructors, functions, and methods.

Place tests beside packages as `*_test.go`, name them `TestXxx`, and prefer table-driven cases. Use fake clocks for deterministic unit tests and real monotonic clocks for E2E tests. Always use the race detector for timer, lease, or shared-state changes. Never hide safety or fencing failures with retries or skips.

## Commits and Pull Requests

Use short imperative commit subjects and keep commits focused. Explain protocol or safety consequences in the body. Pull requests should identify affected packages, link issues or relevant design sections, describe verification performed, and include logs for behavioral changes.
