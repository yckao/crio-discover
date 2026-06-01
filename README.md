# crio-discovery

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

    discovery "github.com/yckao/crio-discovery"
)

func main() {
    cfg := discovery.DefaultConfig()
    d, err := discovery.New(cfg,
        discovery.WithCache("/var/lib/crio-discovery/cache.json", time.Hour),
        discovery.WithPredicate(func(c discovery.Container) bool {
            return c.Kubernetes.Namespace == "default"
        }),
        discovery.WithErrorHandler(func(err error) {
            log.Printf("recoverable discovery error: %v", err)
        }),
    )
    if err != nil {
        log.Fatal(err)
    }

    err = d.Watch(context.Background(), func(ctx context.Context, c discovery.Container) error {
        log.Printf("container %s image=%s volumes=%d", c.ID, c.Image, len(c.Volumes))
        return nil
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

## Notes

- `List` does not use or update the notification cache.
- `Watch` and `WatchChan` use the cache only when `NotificationTTL > 0` and `CachePath` is set.
- Polling remains authoritative even when CRI event acceleration is enabled.
- Volume type inference is best effort and does not use the Kubernetes API.
