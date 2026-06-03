package discover

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRuntimeRetryRetriesTimeoutsWithLimit(t *testing.T) {
	client := &fakeRuntimeClient{
		listErrs: []error{
			status.Error(codes.DeadlineExceeded, "cri timeout 1"),
			context.DeadlineExceeded,
			nil,
		},
		listed: []runtimeContainer{{ID: "running", State: ContainerStateRunning}},
		statusErrs: map[string][]error{
			"running": {
				status.Error(codes.DeadlineExceeded, "cri status timeout"),
				nil,
			},
		},
		statuses: map[string]runtimeContainer{
			"running": {ID: "running", Name: "app", State: ContainerStateRunning},
		},
	}
	cfg := DefaultConfig()
	cfg.RuntimeRetryLimit = 2
	cfg.RuntimeRetryInitialBackoff = time.Nanosecond
	cfg.RuntimeRetryMaxBackoff = time.Nanosecond
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}

	containers, err := d.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(containers) != 1 || containers[0].ID != "running" {
		t.Fatalf("containers = %#v, want running container", containers)
	}
	if client.listCalls != 3 {
		t.Fatalf("list calls = %d, want 3", client.listCalls)
	}
	if len(client.statusCalls) != 2 {
		t.Fatalf("status calls = %d, want 2", len(client.statusCalls))
	}
}

func TestRuntimeRetryStopsAtLimit(t *testing.T) {
	client := &fakeRuntimeClient{
		listErrs: []error{
			status.Error(codes.DeadlineExceeded, "cri timeout 1"),
			status.Error(codes.DeadlineExceeded, "cri timeout 2"),
			status.Error(codes.DeadlineExceeded, "cri timeout 3"),
		},
	}
	cfg := DefaultConfig()
	cfg.RuntimeRetryLimit = 1
	cfg.RuntimeRetryInitialBackoff = time.Nanosecond
	cfg.RuntimeRetryMaxBackoff = time.Nanosecond
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}

	_, err := d.List(context.Background())
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("List error = %v, want deadline exceeded", err)
	}
	if client.listCalls != 2 {
		t.Fatalf("list calls = %d, want initial attempt plus one retry", client.listCalls)
	}
}

func TestRuntimeRetryDoesNotRetryNonTimeoutErrors(t *testing.T) {
	want := errors.New("socket unavailable")
	client := &fakeRuntimeClient{listErrs: []error{want, nil}, listed: []runtimeContainer{{ID: "running", State: ContainerStateRunning}}}
	cfg := DefaultConfig()
	cfg.RuntimeRetryLimit = 2
	cfg.RuntimeRetryInitialBackoff = time.Nanosecond
	cfg.RuntimeRetryMaxBackoff = time.Nanosecond
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}

	_, err := d.List(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("List error = %v, want %v", err, want)
	}
	if client.listCalls != 1 {
		t.Fatalf("list calls = %d, want no retry", client.listCalls)
	}
}
