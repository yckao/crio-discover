package criodiscovery

import (
	"context"
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
