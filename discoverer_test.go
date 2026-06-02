package discover

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		RootPath:       "/var/lib/containers/storage/overlay/rootfs",
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
	if container.RootPath != "/var/lib/containers/storage/overlay/rootfs" {
		t.Fatalf("RootPath = %q, want container rootfs path", container.RootPath)
	}
	if container.Labels["custom"] != "value" || container.Runtime.RawLabels["custom"] != "value" {
		t.Fatalf("labels were not copied: %#v %#v", container.Labels, container.Runtime.RawLabels)
	}
	rc.Labels["custom"] = "changed"
	if container.Labels["custom"] != "value" {
		t.Fatal("container labels must be copied")
	}
}

func TestContainerFromRuntimeUsesSandboxLabelAsPodIDWhenRuntimeOmitsPodSandboxID(t *testing.T) {
	d := &discoverer{config: DefaultConfig(), resolver: staticVolumeResolver{}}
	container := d.containerFromRuntime(runtimeContainer{
		ID:    "container-id",
		Name:  "app",
		State: ContainerStateRunning,
		Labels: map[string]string{
			"io.kubernetes.sandbox.id": "sandbox-from-label",
		},
	})

	if container.PodID != "sandbox-from-label" {
		t.Fatalf("PodID = %q, want sandbox-from-label", container.PodID)
	}
	if container.Kubernetes.SandboxID != "sandbox-from-label" {
		t.Fatalf("Kubernetes.SandboxID = %q, want sandbox-from-label", container.Kubernetes.SandboxID)
	}
}

func TestRuntimeContainerWithFallbackPreservesStatusRootPath(t *testing.T) {
	got := runtimeContainerWithFallback(
		runtimeContainer{ID: "id", RootPath: "/status/rootfs"},
		runtimeContainer{ID: "id", RootPath: "/list/rootfs"},
	)
	if got.RootPath != "/status/rootfs" {
		t.Fatalf("RootPath = %q, want status root path", got.RootPath)
	}
}

func TestRuntimeContainerWithFallbackUsesCandidateRootPath(t *testing.T) {
	got := runtimeContainerWithFallback(
		runtimeContainer{ID: "id"},
		runtimeContainer{ID: "id", RootPath: "/list/rootfs"},
	)
	if got.RootPath != "/list/rootfs" {
		t.Fatalf("RootPath = %q, want fallback root path", got.RootPath)
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

func TestListRejectsNilContext(t *testing.T) {
	d := &discoverer{config: DefaultConfig(), client: &fakeRuntimeClient{}, resolver: staticVolumeResolver{}}
	_, err := d.List(nil)
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("List error = %v, want %v", err, ErrNilContext)
	}
}

func TestCloseCallsRuntimeClient(t *testing.T) {
	client := &fakeRuntimeClient{}
	d := &discoverer{config: DefaultConfig(), client: client, resolver: staticVolumeResolver{}}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	client.mu.Lock()
	closed := client.closed
	client.mu.Unlock()
	if !closed {
		t.Fatal("runtime client was not closed")
	}
}

func TestListUsesCandidateMetadataWhenStatusOmitsLabels(t *testing.T) {
	client := &fakeRuntimeClient{
		listed: []runtimeContainer{{
			ID:           "running",
			PodSandboxID: "sandbox-from-list",
			Name:         "app",
			Image:        "app:v1",
			State:        ContainerStateRunning,
			Labels: map[string]string{
				"io.kubernetes.pod.namespace":  "default",
				"io.kubernetes.pod.name":       "demo",
				"io.kubernetes.pod.uid":        "pod-uid",
				"io.kubernetes.container.name": "app",
			},
		}},
		statuses: map[string]runtimeContainer{
			"running": {
				ID:    "running",
				State: ContainerStateRunning,
				Mounts: []runtimeMount{{
					HostPath:      "/var/lib/kubelet/pods/pod-uid/volumes/kubernetes.io~secret/token",
					ContainerPath: "/var/run/secrets/token",
				}},
			},
		},
	}
	cfg := DefaultConfig()
	cfg.Predicates = []Predicate{func(c Container) bool { return c.Kubernetes.Namespace == "default" }}
	d := &discoverer{config: cfg, client: client, resolver: newKubeletVolumeResolver("/var/lib/kubelet")}

	containers, err := d.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(containers) != 1 {
		t.Fatalf("containers = %#v, want one container", containers)
	}
	got := containers[0]
	if got.PodID != "sandbox-from-list" || got.Kubernetes.SandboxID != "sandbox-from-list" {
		t.Fatalf("sandbox metadata = PodID %q Kubernetes %#v", got.PodID, got.Kubernetes)
	}
	if got.Kubernetes.Namespace != "default" || got.Kubernetes.PodUID != "pod-uid" || got.Kubernetes.ContainerName != "app" {
		t.Fatalf("kubernetes metadata = %#v", got.Kubernetes)
	}
	if got.Image != "app:v1" || got.Name != "app" {
		t.Fatalf("candidate identity fallback failed: %#v", got)
	}
	if len(got.Volumes) != 1 || got.Volumes[0].Type != VolumeTypeSecret || got.Volumes[0].Name != "token" {
		t.Fatalf("volumes = %#v", got.Volumes)
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
		listed: []runtimeContainer{
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
		listed:    []runtimeContainer{{ID: "broken", State: ContainerStateRunning}},
		statusErr: map[string]error{"broken": statusErr},
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

func TestListSkipsContainersGoneBeforeStatus(t *testing.T) {
	client := &fakeRuntimeClient{
		listed: []runtimeContainer{
			{ID: "gone", State: ContainerStateRunning},
			{ID: "keep", State: ContainerStateRunning},
		},
		statuses: map[string]runtimeContainer{
			"keep": {ID: "keep", Name: "app", State: ContainerStateRunning},
		},
		statusErr: map[string]error{"gone": status.Error(codes.NotFound, "container removed")},
	}
	d := &discoverer{config: DefaultConfig(), client: client, resolver: staticVolumeResolver{}}

	containers, err := d.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(containers) != 1 || containers[0].ID != "keep" {
		t.Fatalf("containers = %#v, want only keep", containers)
	}
}

func TestListReturnsRunningMatchingContainers(t *testing.T) {
	client := &fakeRuntimeClient{
		listed: []runtimeContainer{
			{ID: "running", State: ContainerStateRunning},
			{ID: "exited", State: ContainerStateExited},
		},
		statuses: map[string]runtimeContainer{
			"running": {ID: "running", Name: "app", Image: "app:v1", State: ContainerStateRunning, Labels: map[string]string{"io.kubernetes.pod.namespace": "default"}},
			"exited":  {ID: "exited", Name: "old", State: ContainerStateExited},
		},
	}
	d := &discoverer{
		config:   Config{Predicates: []Predicate{func(c Container) bool { return c.Kubernetes.Namespace == "default" }}},
		client:   client,
		resolver: staticVolumeResolver{},
	}
	containers, err := d.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(containers) != 1 || containers[0].ID != "running" || containers[0].Image != "app:v1" {
		t.Fatalf("containers = %#v", containers)
	}
}

func TestListReturnsStatusError(t *testing.T) {
	client := &fakeRuntimeClient{
		listed:    []runtimeContainer{{ID: "running", State: ContainerStateRunning}},
		statuses:  map[string]runtimeContainer{},
		statusErr: map[string]error{"running": errors.New("boom")},
	}
	d := &discoverer{config: DefaultConfig(), client: client, resolver: staticVolumeResolver{}}
	_, err := d.List(context.Background())
	if err == nil || !strings.Contains(err.Error(), "container status running") {
		t.Fatalf("expected status error, got %v", err)
	}
}
