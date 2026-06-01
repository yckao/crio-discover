# crio-discover

Go library for discovering running CRI-O containers through the local Kubernetes CRI socket.

## Features

- One-time `List(ctx)` snapshot of currently running containers.
- Continuous `Watch(ctx, handler)` callback API.
- Continuous `WatchChan(ctx)` channel API.
- Polling-first reliability with optional CRI event acceleration.
- Predicate-only filtering with ordinary Go functions.
- Volume metadata from CRI mounts with best-effort kubelet path inference.
- Persistent TTL cache to suppress duplicate watch notifications across restarts.

## Basic usage

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/yckao/crio-discover"
)

func main() {
    cfg := discover.DefaultConfig()
    d, err := discover.New(cfg,
        discover.WithCache("/var/lib/crio-discover/cache.json", time.Hour),
        discover.WithPredicate(func(c discover.Container) bool {
            return c.Kubernetes.Namespace == "default"
        }),
        discover.WithErrorHandler(func(err error) {
            log.Printf("recoverable discovery error: %v", err)
        }),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer d.Close()

    err = d.Watch(context.Background(), func(ctx context.Context, c discover.Container) error {
        log.Printf("container %s image=%s volumes=%d", c.ID, c.Image, len(c.Volumes))
        return nil
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

## Observability

`WithLogger` emits structured recoverable diagnostics through `log/slog`.
`WithPrometheus` registers low-cardinality metrics with a Prometheus registerer.

```go
registry := prometheus.NewRegistry()
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

d, err := discover.New(discover.DefaultConfig(),
    discover.WithLogger(logger),
    discover.WithPrometheus(registry),
)
```

Metrics avoid Kubernetes labels, annotations, host paths, and full container IDs as labels.

## CRI-O 1.24 compatibility

CRI-O 1.24 supports the CRI `runtime.v1` `ListContainers` and `ContainerStatus` calls used for reliable discovery. It does not support the newer `GetContainerEvents` RPC, so event acceleration is automatically treated as unsupported and polling remains authoritative. You can leave events enabled safely, or use `discover.WithEvents(false)` to avoid the extra unsupported-events probe on CRI-O 1.24-only deployments.

On CRI-O 1.24, newer runtime metadata fields such as image ID and runtime handler may be empty. The library falls back to CRI-O 1.24-era fields and list-time metadata for Kubernetes identity and volume inference.

## Notes

- `List` does not use or update the notification cache.
- `Watch` and `WatchChan` use the cache only when `NotificationTTL > 0` and `CachePath` is set.
- Polling remains authoritative even when CRI event acceleration is enabled.
- Volume type inference is best effort and does not use the Kubernetes API.
