package criodiscovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

var errEventsUnsupported = errors.New("CRI container events unsupported")

func (d *discoverer) Watch(ctx context.Context, handler Handler) error {
	if handler == nil {
		return errors.New("handler is nil")
	}
	return d.watch(ctx, handler, d.reportRecoverable)
}

func (d *discoverer) WatchChan(ctx context.Context) (<-chan Container, <-chan error) {
	containers := make(chan Container)
	errs := make(chan error, 16)
	go func() {
		defer close(containers)
		defer close(errs)
		err := d.watch(ctx, func(ctx context.Context, container Container) error {
			select {
			case containers <- container:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}, func(err error) {
			select {
			case errs <- err:
			case <-ctx.Done():
			}
		})
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			select {
			case errs <- err:
			case <-ctx.Done():
			}
		}
	}()
	return containers, errs
}

func (d *discoverer) watch(ctx context.Context, handler Handler, report func(error)) error {
	if d.client == nil {
		return errors.New("runtime client is nil")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cache, err := newDedupeStore(d.config)
	if err != nil {
		return err
	}
	defer cache.Close()

	if err := d.scanAndHandle(ctx, cache, handler, report, true); err != nil {
		return err
	}

	events := make(chan string, 128)
	if d.config.EnableEvents {
		go d.runEventWatcher(ctx, events, report)
	}

	ticker := time.NewTicker(d.config.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := d.scanAndHandle(ctx, cache, handler, report, false); err != nil {
				return err
			}
		case id := <-events:
			if id == "" {
				continue
			}
			if err := d.handleContainerID(ctx, id, "", cache, handler, report); err != nil {
				return err
			}
		}
	}
}

func (d *discoverer) scanAndHandle(ctx context.Context, cache dedupeStore, handler Handler, report func(error), fatalListError bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	containers, err := d.client.ListContainers(ctx)
	if err != nil {
		if isContextError(err) {
			return err
		}
		wrapped := fmt.Errorf("list containers: %w", err)
		if fatalListError {
			return wrapped
		}
		report(wrapped)
		return nil
	}
	for _, candidate := range containers {
		if err := ctx.Err(); err != nil {
			return err
		}
		if candidate.State != ContainerStateRunning {
			continue
		}
		if err := d.handleContainerID(ctx, candidate.ID, candidate.PodSandboxID, cache, handler, report); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (d *discoverer) handleContainerID(ctx context.Context, id, fallbackSandboxID string, cache dedupeStore, handler Handler, report func(error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	status, err := d.client.ContainerStatus(ctx, id)
	if err != nil {
		if isContextError(err) {
			return err
		}
		report(fmt.Errorf("container status %s: %w", id, err))
		return nil
	}
	if status.State != ContainerStateRunning {
		return nil
	}
	if status.PodSandboxID == "" {
		status.PodSandboxID = fallbackSandboxID
	}
	container := d.containerFromRuntime(status)
	if !d.matches(container) {
		return nil
	}
	if cache.Seen(container.ID) {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := handler(ctx, container); err != nil {
		return err
	}
	if err := cache.Mark(container.ID); err != nil {
		report(fmt.Errorf("mark notification cache %s: %w", container.ID, err))
	}
	return nil
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (d *discoverer) runEventWatcher(ctx context.Context, ids chan<- string, report func(error)) {
	stream, err := d.client.WatchEvents(ctx)
	if err != nil {
		if !errors.Is(err, errEventsUnsupported) {
			report(fmt.Errorf("watch CRI events: %w", err))
		}
		return
	}
	for {
		event, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				return
			}
			report(fmt.Errorf("receive CRI event: %w", err))
			return
		}
		if event.Type != runtimeEventCreated && event.Type != runtimeEventStarted {
			continue
		}
		select {
		case ids <- event.ContainerID:
		case <-ctx.Done():
			return
		}
	}
}

func (d *discoverer) reportRecoverable(err error) {
	if err != nil && d.config.ErrorHandler != nil {
		d.config.ErrorHandler(err)
	}
}
