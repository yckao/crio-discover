package criodiscovery

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWatchEmitsExistingRunningContainerOnce(t *testing.T) {
	client := &fakeRuntimeClient{
		listed: []runtimeContainer{{ID: "running", State: ContainerStateRunning}},
		statuses: map[string]runtimeContainer{
			"running": {ID: "running", Name: "app", State: ContainerStateRunning},
		},
	}
	cfg := DefaultConfig()
	cfg.EnableEvents = false
	cfg.PollInterval = time.Hour
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var got []Container
	err := d.Watch(ctx, func(_ context.Context, c Container) error {
		got = append(got, c)
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch error = %v", err)
	}
	if len(got) != 1 || got[0].ID != "running" {
		t.Fatalf("got = %#v", got)
	}
}

func TestWatchPreservesListedPodSandboxIDWhenStatusOmitsIt(t *testing.T) {
	client := &fakeRuntimeClient{
		listed: []runtimeContainer{{ID: "running", PodSandboxID: "sandbox-from-list", State: ContainerStateRunning}},
		statuses: map[string]runtimeContainer{
			"running": {
				ID:    "running",
				Name:  "app",
				State: ContainerStateRunning,
				Labels: map[string]string{
					"io.kubernetes.pod.namespace":  "default",
					"io.kubernetes.pod.name":       "demo",
					"io.kubernetes.pod.uid":        "pod-uid",
					"io.kubernetes.container.name": "app",
				},
			},
		},
	}
	cfg := DefaultConfig()
	cfg.EnableEvents = false
	cfg.PollInterval = time.Hour
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var got []Container
	err := d.Watch(ctx, func(_ context.Context, c Container) error {
		got = append(got, c)
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got = %#v, want one container", got)
	}
	if got[0].PodID != "sandbox-from-list" {
		t.Fatalf("PodID = %q, want sandbox-from-list", got[0].PodID)
	}
	if got[0].Kubernetes.SandboxID != "sandbox-from-list" {
		t.Fatalf("Kubernetes.SandboxID = %q, want sandbox-from-list", got[0].Kubernetes.SandboxID)
	}
}

func TestWatchSuppressesDuplicateNotificationsAcrossRestart(t *testing.T) {
	cachePath := filepath.Join(t.TempDir(), "cache.json")
	client := &fakeRuntimeClient{
		listed: []runtimeContainer{{ID: "running", State: ContainerStateRunning}},
		statuses: map[string]runtimeContainer{
			"running": {ID: "running", Name: "app", State: ContainerStateRunning},
		},
	}
	cfg := DefaultConfig()
	cfg.EnableEvents = false
	cfg.PollInterval = 10 * time.Millisecond
	cfg.CachePath = cachePath
	cfg.NotificationTTL = time.Hour

	first := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}
	ctx1, cancel1 := context.WithCancel(context.Background())
	count := 0
	err := first.Watch(ctx1, func(context.Context, Container) error {
		count++
		cancel1()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("first watch error = %v", err)
	}
	if count != 1 {
		t.Fatalf("first count = %d", count)
	}

	second := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel2()
	err = second.Watch(ctx2, func(context.Context, Container) error {
		count++
		return nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second watch error = %v", err)
	}
	if count != 1 {
		t.Fatalf("duplicate notification count = %d", count)
	}
}

func TestWatchReportsRecoverablePollingErrors(t *testing.T) {
	client := &fakeRuntimeClient{listErr: errors.New("temporary list failure"), listErrAfter: 1}
	cfg := DefaultConfig()
	cfg.EnableEvents = false
	cfg.PollInterval = 10 * time.Millisecond
	var mu sync.Mutex
	var reported []error
	cfg.ErrorHandler = func(err error) {
		mu.Lock()
		defer mu.Unlock()
		reported = append(reported, err)
	}
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer cancel()
	err := d.Watch(ctx, func(context.Context, Container) error { return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Watch error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reported) == 0 {
		t.Fatal("expected reported polling error")
	}
}

func TestWatchReturnsHandlerError(t *testing.T) {
	want := errors.New("handler failed")
	client := &fakeRuntimeClient{
		listed: []runtimeContainer{{ID: "running", State: ContainerStateRunning}},
		statuses: map[string]runtimeContainer{
			"running": {ID: "running", Name: "app", State: ContainerStateRunning},
		},
	}
	cfg := DefaultConfig()
	cfg.EnableEvents = false
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}
	err := d.Watch(context.Background(), func(context.Context, Container) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("Watch error = %v", err)
	}
}

func TestWatchChanEmitsContainersAndRecoverableErrors(t *testing.T) {
	client := &fakeRuntimeClient{
		listed: []runtimeContainer{{ID: "running", State: ContainerStateRunning}},
		statuses: map[string]runtimeContainer{
			"running": {ID: "running", Name: "app", State: ContainerStateRunning},
		},
	}
	cfg := DefaultConfig()
	cfg.EnableEvents = false
	cfg.PollInterval = time.Hour
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}

	ctx, cancel := context.WithCancel(context.Background())
	containers, errs := d.WatchChan(ctx)
	container, ok := <-containers
	if !ok {
		t.Fatal("containers channel closed before discovery")
	}
	if container.ID != "running" {
		t.Fatalf("container = %#v", container)
	}
	cancel()
	for range containers {
	}
	for err := range errs {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestWatchUsesEventAcceleration(t *testing.T) {
	client := &fakeRuntimeClient{
		listed: []runtimeContainer{},
		statuses: map[string]runtimeContainer{
			"from-event": {ID: "from-event", Name: "app", State: ContainerStateRunning},
		},
		eventStream: &fakeRuntimeEventStream{events: []runtimeEvent{{ContainerID: "from-event", Type: runtimeEventStarted}}},
	}
	cfg := DefaultConfig()
	cfg.EnableEvents = true
	cfg.PollInterval = time.Hour
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan Container, 1)
	errc := make(chan error, 1)
	go func() {
		errc <- d.Watch(ctx, func(_ context.Context, c Container) error {
			got <- c
			cancel()
			return nil
		})
	}()

	select {
	case container := <-got:
		if container.ID != "from-event" {
			t.Fatalf("container = %#v", container)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for event accelerated discovery")
	}
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch error = %v", err)
	}
}

func TestEventWatcherFailureIsRecoverable(t *testing.T) {
	eventErr := errors.New("event stream failed")
	client := &fakeRuntimeClient{
		listed:      []runtimeContainer{},
		statuses:    map[string]runtimeContainer{},
		eventStream: &fakeRuntimeEventStream{err: eventErr},
	}
	cfg := DefaultConfig()
	cfg.EnableEvents = true
	cfg.PollInterval = 10 * time.Millisecond
	reported := make(chan error, 1)
	cfg.ErrorHandler = func(err error) {
		select {
		case reported <- err:
		default:
		}
	}
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		errc <- d.Watch(ctx, func(context.Context, Container) error { return nil })
	}()

	select {
	case err := <-reported:
		if !errors.Is(err, eventErr) {
			t.Fatalf("reported error = %v, want %v", err, eventErr)
		}
	case err := <-errc:
		t.Fatalf("Watch returned before reporting event error: %v", err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event error")
	}
	if err := <-errc; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Watch error = %v", err)
	}
}

func TestEventWatcherSuppressesUnimplementedSetupError(t *testing.T) {
	client := &fakeRuntimeClient{
		listed:   []runtimeContainer{},
		statuses: map[string]runtimeContainer{},
		eventErr: status.Error(codes.Unimplemented, "events unsupported"),
	}
	cfg := DefaultConfig()
	cfg.EnableEvents = true
	cfg.PollInterval = 10 * time.Millisecond
	reported := make(chan error, 1)
	cfg.ErrorHandler = func(err error) {
		select {
		case reported <- err:
		default:
		}
	}
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := d.Watch(ctx, func(context.Context, Container) error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Watch error = %v", err)
	}
	select {
	case err := <-reported:
		t.Fatalf("unexpected reported error: %v", err)
	default:
	}
}

func TestEventWatcherSuppressesUnimplementedReceiveError(t *testing.T) {
	client := &fakeRuntimeClient{
		listed:      []runtimeContainer{},
		statuses:    map[string]runtimeContainer{},
		eventStream: &fakeRuntimeEventStream{err: status.Error(codes.Unimplemented, "events unsupported")},
	}
	cfg := DefaultConfig()
	cfg.EnableEvents = true
	cfg.PollInterval = 10 * time.Millisecond
	reported := make(chan error, 1)
	cfg.ErrorHandler = func(err error) {
		select {
		case reported <- err:
		default:
		}
	}
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := d.Watch(ctx, func(context.Context, Container) error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Watch error = %v", err)
	}
	select {
	case err := <-reported:
		t.Fatalf("unexpected reported error: %v", err)
	default:
	}
}

func TestWatchStopsScanAfterHandlerCancelsContext(t *testing.T) {
	client := &fakeRuntimeClient{
		listed: []runtimeContainer{
			{ID: "first", State: ContainerStateRunning},
			{ID: "second", State: ContainerStateRunning},
		},
		statuses: map[string]runtimeContainer{
			"first":  {ID: "first", Name: "app", State: ContainerStateRunning},
			"second": {ID: "second", Name: "app", State: ContainerStateRunning},
		},
	}
	cfg := DefaultConfig()
	cfg.EnableEvents = false
	cfg.PollInterval = time.Hour
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handlerCalls := 0
	err := d.Watch(ctx, func(_ context.Context, c Container) error {
		handlerCalls++
		if c.ID != "first" {
			t.Fatalf("handled container ID = %q, want first", c.ID)
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch error = %v", err)
	}
	if handlerCalls != 1 {
		t.Fatalf("handler calls = %d, want 1", handlerCalls)
	}

	client.mu.Lock()
	statusCalls := append([]string(nil), client.statusCalls...)
	client.mu.Unlock()
	if len(statusCalls) != 1 || statusCalls[0] != "first" {
		t.Fatalf("status calls = %v, want [first]", statusCalls)
	}
}

func TestWatchTreatsStatusContextErrorAsTerminal(t *testing.T) {
	client := &fakeRuntimeClient{
		listed:    []runtimeContainer{{ID: "running", State: ContainerStateRunning}},
		statusErr: map[string]error{"running": context.Canceled},
	}
	cfg := DefaultConfig()
	cfg.EnableEvents = false
	reported := 0
	cfg.ErrorHandler = func(error) { reported++ }
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}

	handlerCalls := 0
	err := d.Watch(context.Background(), func(context.Context, Container) error {
		handlerCalls++
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch error = %v, want context.Canceled", err)
	}
	if reported != 0 {
		t.Fatalf("reported recoverable errors = %d, want 0", reported)
	}
	if handlerCalls != 0 {
		t.Fatalf("handler calls = %d, want 0", handlerCalls)
	}
}

func TestIsContextErrorRecognizesGRPCStatusCodes(t *testing.T) {
	for _, err := range []error{
		status.Error(codes.Canceled, "canceled"),
		status.Error(codes.DeadlineExceeded, "deadline exceeded"),
	} {
		if !isContextError(err) {
			t.Fatalf("isContextError(%v) = false, want true", err)
		}
	}
}

func TestWatchCancelsEventWatcherContextOnHandlerError(t *testing.T) {
	want := errors.New("handler failed")
	stream := newCancelAwareEventStream(runtimeEvent{ContainerID: "evented", Type: runtimeEventStarted})
	client := &cancelAwareEventClient{
		fakeRuntimeClient: fakeRuntimeClient{
			statuses: map[string]runtimeContainer{
				"evented": {ID: "evented", Name: "app", State: ContainerStateRunning},
			},
		},
		stream: stream,
	}
	cfg := DefaultConfig()
	cfg.EnableEvents = true
	cfg.PollInterval = time.Hour
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := d.Watch(ctx, func(context.Context, Container) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("Watch error = %v, want %v", err, want)
	}
	select {
	case <-stream.canceled:
	case <-time.After(time.Second):
		t.Fatal("event watcher context was not canceled")
	}
}

type cancelAwareEventClient struct {
	fakeRuntimeClient
	stream *cancelAwareEventStream
}

func (c *cancelAwareEventClient) WatchEvents(ctx context.Context) (runtimeEventStream, error) {
	c.stream.setContext(ctx)
	return c.stream, nil
}

type cancelAwareEventStream struct {
	event runtimeEvent

	ctx       context.Context
	sent      bool
	canceled  chan struct{}
	closeOnce sync.Once
}

func newCancelAwareEventStream(event runtimeEvent) *cancelAwareEventStream {
	return &cancelAwareEventStream{event: event, canceled: make(chan struct{})}
}

func (s *cancelAwareEventStream) setContext(ctx context.Context) {
	s.ctx = ctx
}

func (s *cancelAwareEventStream) Recv() (runtimeEvent, error) {
	if !s.sent {
		s.sent = true
		return s.event, nil
	}
	if s.ctx == nil {
		return runtimeEvent{}, errors.New("event stream context not set")
	}
	<-s.ctx.Done()
	s.closeOnce.Do(func() { close(s.canceled) })
	return runtimeEvent{}, s.ctx.Err()
}
