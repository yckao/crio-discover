# CRI-O Container Discovery Go Library Design

Date: 2026-06-01

## Summary

Build a Go library package for discovering running CRI-O containers through the local CRI socket. The library provides one-time listing and continuous watching APIs, enriches discovered containers with useful metadata and volume details, supports caller-defined predicate filtering, and uses a persistent TTL cache to suppress duplicate watch notifications across process restarts.

## Goals

- Discover currently running containers from CRI-O using the CRI runtime service over a local socket.
- Include containers that were already running before discovery starts.
- Provide reliable continuous discovery through polling, with optional CRI event acceleration for lower latency.
- Return necessary metadata, including Kubernetes identity fields when available.
- Return volume/mount metadata including host path, container path, read-only status, propagation, name, and best-effort type inference.
- Support flexible filtering through caller-provided predicates.
- Persist notification dedupe state to a file so recent notifications are not repeated after restart.
- Expose both callback and channel-based watch APIs, plus a one-time list API.

## Non-goals

- Implement a daemon, CLI, or Kubernetes controller in the first version.
- Depend on Kubernetes API credentials for volume or pod metadata enrichment.
- Provide built-in include/exclude/regex filter DSLs in the first version.
- Guarantee exact Kubernetes volume source type for every mount. Type inference is best effort.

## Architecture

The library centers on an exported `Discoverer` interface that connects to the CRI-O CRI socket, scans running containers, enriches each container, applies predicates, and emits deduplicated discoveries. The default implementation is unexported so library users can mock the interface in their own tests.

High-level flow for continuous watching:

1. Connect to CRI-O through the configured local CRI socket, defaulting to `/var/run/crio/crio.sock`.
2. Load the persistent TTL notification cache from disk.
3. Run an initial scan of existing running containers.
4. Continue polling `ListContainers` on a configurable interval.
5. If enabled and supported, run a CRI event watcher in parallel to accelerate discovery between polls.
6. For each candidate running container:
   - fetch detailed CRI container status;
   - normalize metadata;
   - enrich volume information with CRI mount data and kubelet filesystem inference;
   - apply all caller predicates;
   - check the persistent TTL notification cache;
   - emit only if the container was not notified within the TTL;
   - persist the notification marker after successful emission.
7. If the event watcher fails, report the error but keep polling. Polling remains authoritative.

Main internal components:

- `Discoverer`: exported public interface for listing and watching containers.
- unexported discoverer implementation: lifecycle coordinator returned by `New`.
- `CRIClient`: small internal interface wrapping the CRI runtime service calls used by the library.
- `watchEngine`: coordinates startup scan, polling, optional events, dedupe, and emission.
- `Cache`: persistent TTL dedupe store.
- `Predicate`: caller-provided filter function.
- `VolumeResolver`: combines CRI mounts with kubelet filesystem inference.

## Public API

The library exposes one constructor style with config plus functional options. `Discoverer` is an interface to make mocking straightforward for library users:

```go
type Discoverer interface {
    List(ctx context.Context) ([]Container, error)
    Watch(ctx context.Context, handler Handler) error
    WatchChan(ctx context.Context) (<-chan Container, <-chan error)
    Close() error
}

func New(config Config, opts ...Option) (Discoverer, error)
func DefaultConfig() Config

type Handler func(context.Context, Container) error
type ErrorHandler func(error)
```

The concrete implementation returned by `New` is unexported.

API semantics:

- `List`
  - performs a one-time snapshot of currently running containers;
  - enriches metadata and volumes;
  - applies predicates;
  - does not read or update the notification cache;
  - returns all matching running containers.

- `Watch`
  - callback-based continuous discovery;
  - performs startup scan, polling, and optional event acceleration;
  - enriches metadata and volumes;
  - applies predicates;
  - uses the persistent TTL cache to suppress duplicate notifications;
  - invokes the handler only for newly notifiable containers;
  - sends recoverable polling, event, and cache-write errors to the optional configured `ErrorHandler` while continuing when safe;
  - returns when the context is canceled, setup fails fatally, or the handler returns an error.

- `WatchChan`
  - channel-based wrapper around the same watch engine;
  - returns a container channel and an error channel;
  - closes channels when the context is canceled or the watch engine exits.

- `Close`
  - releases the underlying runtime client resources;
  - should be called when the discoverer is no longer needed.

Functional options:

```go
type Option func(*Config)

func WithPredicate(p Predicate) Option
func WithPredicates(p ...Predicate) Option
func WithPollInterval(d time.Duration) Option
func WithEvents(enabled bool) Option
func WithCache(path string, ttl time.Duration) Option
func WithErrorHandler(h ErrorHandler) Option
```

## Configuration

```go
type Config struct {
    CRISocketPath   string
    KubeletRoot     string
    PollInterval    time.Duration
    EnableEvents    bool
    NotificationTTL time.Duration
    CachePath       string
    Predicates      []Predicate
    ErrorHandler    ErrorHandler
}
```

Recommended defaults:

- `CRISocketPath`: `/var/run/crio/crio.sock`
- `KubeletRoot`: `/var/lib/kubelet`
- `PollInterval`: `30s`
- `EnableEvents`: `true`
- `NotificationTTL`: `0`, meaning dedupe cache disabled unless explicitly configured
- `CachePath`: empty by default
- `Predicates`: empty, meaning accept all discovered running containers
- `ErrorHandler`: nil, meaning recoverable callback-watch errors are ignored

Validation rules:

- `PollInterval` must be positive for watching.
- If `NotificationTTL > 0`, `CachePath` should be set. Returning a config error is preferred over silently using a non-persistent cache because persistence is an explicit requirement.
- Predicate functions must be non-nil.
- If `ErrorHandler` is set, it must be non-nil.

## Data model

```go
type Container struct {
    ID          string
    PodID       string
    Name        string
    Image       string
    ImageRef    string
    State       ContainerState
    CreatedAt   time.Time

    Labels      map[string]string
    Annotations map[string]string

    Kubernetes  KubernetesMetadata
    Runtime     RuntimeMetadata
    Volumes     []Volume
}
```

Kubernetes metadata is parsed from common CRI labels and annotations when present:

```go
type KubernetesMetadata struct {
    Namespace     string
    PodName       string
    PodUID        string
    ContainerName string
    SandboxID     string
    Attempt       uint32
}
```

Runtime metadata preserves runtime-specific details without forcing all fields into stable top-level API fields:

```go
type RuntimeMetadata struct {
    RuntimeHandler string
    ImageID        string
    RawLabels      map[string]string
    RawAnnotations map[string]string
}
```

Container state should be normalized enough for filtering and display, while retaining raw CRI information internally where useful.

## Volume metadata and inference

```go
type Volume struct {
    HostPath      string
    ContainerPath string
    ReadOnly      bool
    Propagation   string
    Type          VolumeType
    Name          string
    Source        string
}
```

Supported best-effort volume types:

- `VolumeTypeHostPath`
- `VolumeTypeEmptyDir`
- `VolumeTypeSecret`
- `VolumeTypeConfigMap`
- `VolumeTypeProjected`
- `VolumeTypePersistentVolumeClaim`
- `VolumeTypeDownwardAPI`
- `VolumeTypeUnknown`

Volume resolution uses two inputs:

1. CRI mount data from container status, which provides host path, container path, read-only flag, and propagation where available.
2. Kubelet filesystem layout under the configured `KubeletRoot`, usually `/var/lib/kubelet`, to infer volume name and type from paths such as pod volume plugin directories.

Inference failures must not drop containers. If type or name cannot be inferred, the library still returns the CRI mount with `VolumeTypeUnknown` and empty optional fields.

## Filtering

Filtering is predicate-only:

```go
type Predicate func(Container) bool
```

Rules:

- If no predicates are configured, all discovered running containers match.
- If predicates are configured, every predicate must return true.
- Predicate evaluation happens after metadata and volume enrichment so callers can filter on volumes as well as standard metadata.
- The library does not include built-in include/exclude lists or regex rules in the first version. Callers can implement those in normal Go closures.

Example:

```go
d, err := criodiscover.New(criodiscover.DefaultConfig(),
    criodiscover.WithPredicate(func(c criodiscover.Container) bool {
        return c.Kubernetes.Namespace == "default"
    }),
    criodiscover.WithPredicate(func(c criodiscover.Container) bool {
        return !strings.HasPrefix(c.Image, "pause:")
    }),
)
```

## Notification dedupe cache

The cache exists only for watch notification deduplication. It is not the source of truth for discovered containers and is not used by `List`.

Behavior:

- Keyed by container ID.
- Stores last-notified timestamp and expiry timestamp.
- On watch startup, loads the cache file and prunes expired entries.
- Before emitting a container, checks whether the container ID has an unexpired entry.
- After successful emission, records a new expiry based on `NotificationTTL` and persists the cache.
- Uses atomic file writes to avoid partial cache corruption.

Failure policy:

- Cache load failures are returned as setup errors when persistent dedupe is enabled.
- Cache write failures are reported and should prevent marking a container as notified. This avoids silently missing notifications after restart.
- Corrupt cache files should produce explicit errors rather than silently discarding data.

Suggested JSON shape:

```json
{
  "version": 1,
  "entries": {
    "container-id": {
      "last_notified_at": "2026-06-01T00:00:00Z",
      "expires_at": "2026-06-01T01:00:00Z"
    }
  }
}
```

## Event acceleration and polling reliability

Polling is the reliability baseline. It handles:

- containers that existed before discovery started;
- missed CRI events;
- event stream interruptions;
- CRI-O versions where event support differs.

Event acceleration is optional and best effort:

- If enabled, the library listens for CRI container events when supported.
- Event-discovered candidates go through the same status fetch, enrichment, filtering, and cache checks as polling-discovered candidates.
- Event watcher errors are sent to the error path but do not stop polling.
- Polling periodically reconciles state and remains authoritative.

## Error handling

- Constructor/config validation errors are returned from `New`.
- Fatal setup errors, such as inability to connect to the CRI socket or load a required cache file, are returned from `Watch` or sent through `WatchChan` before shutdown.
- Polling errors are recoverable and reported through `WatchChan`'s error channel or the configured `ErrorHandler` for callback `Watch`.
- Event watcher errors are recoverable, reported through the same error path, and polling continues.
- Volume inference errors are non-fatal and result in unknown volume metadata.
- Handler errors stop `Watch` and are returned to the caller.

## Testing strategy

Unit tests:

- Config defaults and validation.
- Predicate evaluation semantics.
- TTL cache load, save, prune, expiry, and atomic-write behavior.
- Cache behavior across simulated restart.
- Kubelet path volume type inference for common volume plugin paths.
- Unknown volume inference fallback.

Fake CRI client tests:

- `List` returns currently running matching containers.
- `List` does not read or update the cache.
- `Watch` emits containers that existed before startup.
- Polling discovers newly running containers.
- Event acceleration emits faster than the next poll.
- Duplicate notifications are suppressed within TTL.
- Expired TTL allows a later notification.
- Event watcher failure does not stop polling.
- Predicate filtering happens after volume enrichment.

Integration tests, when a CRI-O environment is available:

- Connect to a real CRI-O socket.
- Discover a running test container.
- Verify host/container mount paths are returned.
- Verify cache survives process restart.

## Open implementation notes

- Keep the CRI client behind a small interface so fake clients can drive tests without a live CRI-O runtime.
- Prefer stable exported structs for normalized metadata, and keep runtime-specific details in maps or internal raw fields.
- Avoid Kubernetes API dependencies in the initial package.
- Keep event support optional so CRI-O environments without compatible event behavior still work reliably.
