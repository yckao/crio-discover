package discover

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errEventsUnsupported = errors.New("CRI container events unsupported")

type watchEventMessage struct {
	containerID string
	err         error
}

func (d *discoverer) Watch(ctx context.Context, handler Handler) error {
	if ctx == nil {
		return ErrNilContext
	}
	if handler == nil {
		return ErrNilHandler
	}
	return d.watch(ctx, handler, d.reportRecoverable)
}

func (d *discoverer) WatchChan(ctx context.Context) (<-chan Container, <-chan error) {
	containers := make(chan Container)
	errs := make(chan error, 16)
	if ctx == nil {
		close(containers)
		errs <- ErrNilContext
		close(errs)
		return containers, errs
	}
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
			sendErrorNonBlocking(ctx, errs, err)
		})
		if err != nil && !isContextError(err) {
			sendErrorNonBlocking(ctx, errs, err)
		}
	}()
	return containers, errs
}

func (d *discoverer) watch(ctx context.Context, handler Handler, report func(error)) error {
	if ctx == nil {
		return ErrNilContext
	}
	if d.client == nil {
		return ErrMissingRuntimeClient
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cache, err := newDedupeStore(d.config)
	if err != nil {
		d.reportRecoverableTo(ctx, report, "cache_load", "", err)
		d.observeCacheEvent("load_error")
		if d.config.NotificationTTL > 0 {
			cache = newMemoryDedupeStore(d.config.NotificationTTL, time.Now)
		} else {
			cache = newNoopDedupeStore()
		}
	} else {
		d.observeCacheEvent("load_success")
	}
	defer cache.Close()

	if err := d.scanAndHandle(ctx, cache, handler, report, true, "initial_scan"); err != nil {
		return err
	}

	events := make(chan watchEventMessage, 128)
	if d.config.EnableEvents {
		go d.runEventWatcher(ctx, events)
	}

	ticker := time.NewTicker(d.config.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := d.scanAndHandle(ctx, cache, handler, report, false, "poll"); err != nil {
				return err
			}
		case event := <-events:
			if event.err != nil {
				d.reportRecoverableTo(ctx, report, "event_stream", "", event.err)
				continue
			}
			if event.containerID == "" {
				continue
			}
			if err := d.handleContainerID(ctx, event.containerID, runtimeContainer{}, cache, handler, report, "event"); err != nil {
				return err
			}
		}
	}
}

func (d *discoverer) scanAndHandle(ctx context.Context, cache dedupeStore, handler Handler, report func(error), fatalListError bool, source string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	containers, err := d.listRuntimeContainers(ctx)
	if err != nil {
		if isContextError(err) {
			return err
		}
		wrapped := fmt.Errorf("list containers: %w", err)
		if fatalListError {
			d.observeScan(source, "error")
			return wrapped
		}
		d.observeScan(source, "recoverable_error")
		d.reportRecoverableTo(ctx, report, "list_containers", "", wrapped)
		return nil
	}
	d.observeScan(source, "success")
	for _, candidate := range containers {
		if err := ctx.Err(); err != nil {
			return err
		}
		if candidate.State != ContainerStateRunning {
			continue
		}
		if err := d.handleContainerID(ctx, candidate.ID, candidate, cache, handler, report, source); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (d *discoverer) handleContainerID(ctx context.Context, id string, fallback runtimeContainer, cache dedupeStore, handler Handler, report func(error), source string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	status, err := d.runtimeContainerStatus(ctx, id)
	if err != nil {
		if isContextError(err) {
			return err
		}
		if isContainerGoneError(err) {
			d.observeCacheEvent("container_gone")
			return nil
		}
		d.reportRecoverableTo(ctx, report, "container_status", id, fmt.Errorf("container status %s: %w", id, err))
		return nil
	}
	if status.State != ContainerStateRunning {
		return nil
	}
	status = runtimeContainerWithFallback(status, fallback)
	container := d.containerFromRuntime(status)
	if !d.matches(container) {
		return nil
	}
	if cache.Seen(container.ID) {
		d.observeCacheEvent("hit")
		return nil
	}
	d.observeCacheEvent("miss")
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := handler(ctx, container); err != nil {
		return err
	}
	d.observeNotification(source)
	if err := cache.Mark(container.ID); err != nil {
		d.observeCacheEvent("mark_error")
		d.reportRecoverableTo(ctx, report, "cache_mark", container.ID, fmt.Errorf("mark notification cache %s: %w", container.ID, err))
	} else {
		d.observeCacheEvent("mark_success")
	}
	return nil
}

func isContextError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

func isEventsUnsupported(err error) bool {
	return errors.Is(err, errEventsUnsupported) || status.Code(err) == codes.Unimplemented
}

func isContainerGoneError(err error) bool {
	return status.Code(err) == codes.NotFound
}

const (
	eventWatcherInitialBackoff = 50 * time.Millisecond
	eventWatcherMaxBackoff     = time.Second
)

func (d *discoverer) runEventWatcher(ctx context.Context, events chan<- watchEventMessage) {
	backoff := eventWatcherInitialBackoff
	for {
		retry, reportErr := d.runEventWatcherOnce(ctx, events)
		if !retry {
			return
		}
		if reportErr != nil && !sendWatchEvent(ctx, events, watchEventMessage{err: reportErr}) {
			return
		}
		if !sleepContext(ctx, backoff) {
			return
		}
		backoff *= 2
		if backoff > eventWatcherMaxBackoff {
			backoff = eventWatcherMaxBackoff
		}
	}
}

func (d *discoverer) runEventWatcherOnce(ctx context.Context, events chan<- watchEventMessage) (bool, error) {
	stream, err := d.runtimeWatchEvents(ctx)
	if err != nil {
		if ctx.Err() != nil || isContextError(err) {
			return false, nil
		}
		if isEventsUnsupported(err) {
			d.observeEventStreamEvent(ctx, "unsupported", nil)
			return false, nil
		}
		d.observeEventStreamEvent(ctx, "setup_error", err)
		return true, fmt.Errorf("watch CRI events: %w", err)
	}
	d.observeEventStreamEvent(ctx, "started", nil)
	for {
		event, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil || isContextError(err) {
				return false, nil
			}
			if isEventsUnsupported(err) {
				d.observeEventStreamEvent(ctx, "unsupported", nil)
				return false, nil
			}
			if errors.Is(err, io.EOF) {
				d.observeEventStreamEvent(ctx, "closed", nil)
				return true, nil
			}
			d.observeEventStreamEvent(ctx, "receive_error", err)
			return true, fmt.Errorf("receive CRI event: %w", err)
		}
		if event.Type != runtimeEventCreated && event.Type != runtimeEventStarted {
			d.observeEventStreamEvent(ctx, "ignored_event", nil)
			continue
		}
		d.observeEventStreamEvent(ctx, "received_event", nil)
		if !sendWatchEvent(ctx, events, watchEventMessage{containerID: event.ContainerID}) {
			return false, nil
		}
	}
}

func sendWatchEvent(ctx context.Context, events chan<- watchEventMessage, event watchEventMessage) bool {
	select {
	case events <- event:
		return true
	case <-ctx.Done():
		return false
	}
}

func sendErrorNonBlocking(ctx context.Context, errs chan error, err error) {
	if err == nil {
		return
	}
	select {
	case errs <- err:
		return
	case <-ctx.Done():
		return
	default:
	}
	select {
	case <-errs:
	default:
	}
	select {
	case errs <- err:
	case <-ctx.Done():
	default:
	}
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (d *discoverer) reportRecoverable(err error) {
	if err != nil && d.config.ErrorHandler != nil {
		d.config.ErrorHandler(err)
	}
}
