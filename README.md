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
    cfg := criodiscover.DefaultConfig()
    d, err := criodiscover.New(cfg,
        criodiscover.WithCache("/var/lib/crio-discover/cache.json", time.Hour),
        criodiscover.WithPredicate(func(c criodiscover.Container) bool {
            return c.Kubernetes.Namespace == "default"
        }),
        criodiscover.WithErrorHandler(func(err error) {
            log.Printf("recoverable discovery error: %v", err)
        }),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer d.Close()

    err = d.Watch(context.Background(), func(ctx context.Context, c criodiscover.Container) error {
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
