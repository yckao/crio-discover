package criodiscovery

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestContainerFromRuntimeParsesKubernetesMetadata(t *testing.T) {
	d := &discoverer{
		config:   DefaultConfig(),
		resolver: staticVolumeResolver{},
	}
	created := time.Unix(100, 20)
	rc := runtimeContainer{
		ID:             "container-id",
		PodSandboxID:   "sandbox-id",
		Name:           "app",
		Attempt:        2,
		Image:          "registry/app:v1",
		ImageRef:       "sha256:abc",
		ImageID:        "image-id",
		RuntimeHandler: "runc",
		State:          ContainerStateRunning,
		CreatedAt:      created,
		Labels: map[string]string{
			"io.kubernetes.pod.namespace":  "default",
			"io.kubernetes.pod.name":       "demo",
			"io.kubernetes.pod.uid":        "pod-uid",
			"io.kubernetes.container.name": "app",
			"io.kubernetes.sandbox.id":     "sandbox-id",
			"custom":                       "value",
		},
		Annotations: map[string]string{"annotation": "value"},
	}

	container := d.containerFromRuntime(rc)
	if container.ID != "container-id" || container.PodID != "sandbox-id" || container.Name != "app" {
		t.Fatalf("unexpected identity: %#v", container)
	}
	if container.Kubernetes.Namespace != "default" || container.Kubernetes.PodName != "demo" || container.Kubernetes.PodUID != "pod-uid" {
		t.Fatalf("unexpected kubernetes metadata: %#v", container.Kubernetes)
	}
	if container.Kubernetes.ContainerName != "app" || container.Kubernetes.SandboxID != "sandbox-id" || container.Kubernetes.Attempt != 2 {
		t.Fatalf("unexpected kubernetes container fields: %#v", container.Kubernetes)
	}
	if container.Runtime.RuntimeHandler != "runc" || container.Runtime.ImageID != "image-id" {
		t.Fatalf("unexpected runtime metadata: %#v", container.Runtime)
	}
	if container.Labels["custom"] != "value" || container.Runtime.RawLabels["custom"] != "value" {
		t.Fatalf("labels were not copied: %#v %#v", container.Labels, container.Runtime.RawLabels)
	}
	rc.Labels["custom"] = "changed"
	if container.Labels["custom"] != "value" {
		t.Fatal("container labels must be copied")
	}
}

func TestMatchesRequiresAllPredicates(t *testing.T) {
	d := &discoverer{config: Config{Predicates: []Predicate{
		func(c Container) bool { return c.Kubernetes.Namespace == "default" },
		func(c Container) bool { return c.Image == "app:v1" },
	}}}
	if !d.matches(Container{Image: "app:v1", Kubernetes: KubernetesMetadata{Namespace: "default"}}) {
		t.Fatal("expected match")
	}
	if d.matches(Container{Image: "app:v2", Kubernetes: KubernetesMetadata{Namespace: "default"}}) {
		t.Fatal("expected non-match")
	}
}

type staticVolumeResolver struct{}

func (staticVolumeResolver) Resolve(string, []runtimeMount) []Volume {
	return []Volume{{HostPath: "/host", ContainerPath: "/container", Type: VolumeTypeUnknown}}
}

func TestNewReturnsDiscovererInterface(t *testing.T) {
	cfg := DefaultConfig()
	discoverer, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if discoverer == nil {
		t.Fatal("discoverer is nil")
	}
}

func TestListRequiresRuntimeClient(t *testing.T) {
	d := &discoverer{config: DefaultConfig(), resolver: staticVolumeResolver{}}
	_, err := d.List(context.Background())
	if err == nil {
		t.Fatal("expected missing runtime client error")
	}
}

func TestNewDefaultConfigListFailsClosedWhileRuntimeGRPCStubbed(t *testing.T) {
	discoverer, err := New(DefaultConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = discoverer.List(context.Background())
	if !errors.Is(err, errRuntimeClientNotImplemented) {
		t.Fatalf("expected runtime client not implemented error, got %v", err)
	}
}

func TestWatchChanReturnsNotImplementedError(t *testing.T) {
	d := &discoverer{}
	_, errs := d.WatchChan(context.Background())

	select {
	case err, ok := <-errs:
		if !ok {
			t.Fatal("expected error channel value")
		}
		if !errors.Is(err, errWatchNotImplemented) {
			t.Fatalf("expected watch not implemented error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch error")
	}

	if err := d.Watch(context.Background(), nil); !errors.Is(err, errWatchNotImplemented) {
		t.Fatalf("expected Watch to return watch not implemented error, got %v", err)
	}
}

func TestListFiltersCandidatesStatusesAndPredicates(t *testing.T) {
	labels := func(namespace, containerName string) map[string]string {
		return map[string]string{
			"io.kubernetes.pod.namespace":  namespace,
			"io.kubernetes.pod.name":       "demo",
			"io.kubernetes.pod.uid":        "pod-uid",
			"io.kubernetes.container.name": containerName,
		}
	}
	cfg := DefaultConfig()
	cfg.Predicates = []Predicate{
		func(c Container) bool { return c.Kubernetes.Namespace == "target" },
	}
	client := &fakeRuntimeClient{
		containers: []runtimeContainer{
			{ID: "keep", PodSandboxID: "sandbox-keep", State: ContainerStateRunning},
			{ID: "stopped-candidate", PodSandboxID: "sandbox-stopped", State: ContainerStateExited},
			{ID: "filtered", PodSandboxID: "sandbox-filtered", State: ContainerStateRunning},
			{ID: "status-exited", PodSandboxID: "sandbox-status-exited", State: ContainerStateRunning},
		},
		statuses: map[string]runtimeContainer{
			"keep": {
				ID:             "keep",
				Name:           "app",
				Image:          "registry.example/app:v1",
				ImageRef:       "sha256:keep",
				ImageID:        "image-keep",
				RuntimeHandler: "runc",
				State:          ContainerStateRunning,
				CreatedAt:      time.Unix(200, 0),
				Labels:         labels("target", "app"),
			},
			"filtered": {
				ID:        "filtered",
				Name:      "sidecar",
				Image:     "registry.example/sidecar:v1",
				State:     ContainerStateRunning,
				CreatedAt: time.Unix(201, 0),
				Labels:    labels("other", "sidecar"),
			},
			"status-exited": {
				ID:        "status-exited",
				Name:      "old",
				Image:     "registry.example/old:v1",
				State:     ContainerStateExited,
				CreatedAt: time.Unix(202, 0),
				Labels:    labels("target", "old"),
			},
		},
	}
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}

	containers, err := d.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(containers) != 1 {
		t.Fatalf("containers = %d, want 1: %#v", len(containers), containers)
	}
	got := containers[0]
	if got.ID != "keep" || got.Name != "app" || got.Image != "registry.example/app:v1" {
		t.Fatalf("unexpected container: %#v", got)
	}
	if got.PodID != "sandbox-keep" {
		t.Fatalf("PodID = %q, want candidate sandbox", got.PodID)
	}
	if got.Kubernetes.SandboxID != "sandbox-keep" {
		t.Fatalf("Kubernetes.SandboxID = %q, want candidate sandbox", got.Kubernetes.SandboxID)
	}
	if got.Kubernetes.Namespace != "target" || got.Kubernetes.ContainerName != "app" {
		t.Fatalf("unexpected kubernetes metadata: %#v", got.Kubernetes)
	}

	wantStatusCalls := []string{"keep", "filtered", "status-exited"}
	if len(client.statusCalls) != len(wantStatusCalls) {
		t.Fatalf("status calls = %#v, want %#v", client.statusCalls, wantStatusCalls)
	}
	for i := range wantStatusCalls {
		if client.statusCalls[i] != wantStatusCalls[i] {
			t.Fatalf("status calls = %#v, want %#v", client.statusCalls, wantStatusCalls)
		}
	}
}

func TestListWrapsStatusErrorsWithContainerID(t *testing.T) {
	statusErr := errors.New("status failed")
	client := &fakeRuntimeClient{
		containers: []runtimeContainer{{ID: "broken", State: ContainerStateRunning}},
		statusErrs: map[string]error{"broken": statusErr},
	}
	d := &discoverer{config: DefaultConfig(), client: client, resolver: staticVolumeResolver{}}

	_, err := d.List(context.Background())
	if err == nil {
		t.Fatal("expected status error")
	}
	if !errors.Is(err, statusErr) {
		t.Fatalf("expected wrapped status error, got %v", err)
	}
	if !strings.Contains(err.Error(), "container status broken") {
		t.Fatalf("error %q does not contain container status ID", err.Error())
	}
}

type fakeRuntimeClient struct {
	containers  []runtimeContainer
	statuses    map[string]runtimeContainer
	statusErrs  map[string]error
	listErr     error
	statusCalls []string
}

func (f *fakeRuntimeClient) ListContainers(context.Context) ([]runtimeContainer, error) {
	return f.containers, f.listErr
}

func (f *fakeRuntimeClient) ContainerStatus(_ context.Context, id string) (runtimeContainer, error) {
	f.statusCalls = append(f.statusCalls, id)
	if err := f.statusErrs[id]; err != nil {
		return runtimeContainer{}, err
	}
	status, ok := f.statuses[id]
	if !ok {
		return runtimeContainer{}, errors.New("missing container status")
	}
	return status, nil
}

func (f *fakeRuntimeClient) WatchEvents(context.Context) (runtimeEventStream, error) {
	return nil, errEventsUnsupported
}

func (f *fakeRuntimeClient) Close() error { return nil }
