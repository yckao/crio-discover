package criodiscovery

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
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
