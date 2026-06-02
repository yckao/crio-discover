package discover

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (d *discoverer) listRuntimeContainers(ctx context.Context) ([]runtimeContainer, error) {
	return retryRuntimeRPC(ctx, d.config, func() ([]runtimeContainer, error) {
		started := time.Now()
		containers, err := d.client.ListContainers(ctx)
		d.observeRuntimeRPC("ListContainers", started, err)
		return containers, err
	})
}

func (d *discoverer) runtimeContainerStatus(ctx context.Context, id string) (runtimeContainer, error) {
	return retryRuntimeRPC(ctx, d.config, func() (runtimeContainer, error) {
		started := time.Now()
		container, err := d.client.ContainerStatus(ctx, id)
		d.observeRuntimeRPC("ContainerStatus", started, err)
		return container, err
	})
}

func (d *discoverer) runtimeWatchEvents(ctx context.Context) (runtimeEventStream, error) {
	return retryRuntimeRPC(ctx, d.config, func() (runtimeEventStream, error) {
		started := time.Now()
		stream, err := d.client.WatchEvents(ctx)
		d.observeRuntimeRPC("GetContainerEvents", started, err)
		return stream, err
	})
}

func retryRuntimeRPC[T any](ctx context.Context, config Config, call func() (T, error)) (T, error) {
	var zero T
	backoff := config.RuntimeRetryInitialBackoff
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		value, err := call()
		if err == nil {
			return value, nil
		}
		if attempt >= config.RuntimeRetryLimit || !isRuntimeRetryableError(err) || ctx.Err() != nil {
			return value, err
		}
		if !sleepContext(ctx, backoff) {
			return zero, ctx.Err()
		}
		backoff *= 2
		if backoff > config.RuntimeRetryMaxBackoff {
			backoff = config.RuntimeRetryMaxBackoff
		}
	}
}

func isRuntimeRetryableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return status.Code(err) == codes.DeadlineExceeded
}
