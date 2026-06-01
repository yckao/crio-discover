# CRI-O Discover Go Library Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `github.com/yckao/crio-discover`, a Go library that lists and watches running CRI-O containers through the local CRI socket with metadata, volume inference, predicate filtering, event acceleration, and persistent TTL notification dedupe.

**Architecture:** The package exposes an exported `Discoverer` interface returned by `New(Config, ...Option)`. The unexported implementation uses a small internal CRI runtime client interface, a kubelet-volume resolver, and a file-backed TTL cache. Polling is authoritative; CRI events only accelerate discovery between polls.

**Tech Stack:** Go 1.24, `k8s.io/cri-api` v0.34.8, gRPC v1.72.1, standard-library JSON/file APIs, Go unit tests with fake runtime clients.

---

## File Structure

- Create `go.mod`: module definition and dependencies.
- Create `doc.go`: package documentation.
- Create `types.go`: exported API types, metadata structs, state/type constants.
- Create `options.go`: defaults, functional options, config validation.
- Create `runtime_client.go`: internal CRI abstraction used by production and fake clients.
- Create `discoverer.go`: constructor, metadata normalization, predicate evaluation, `List`.
- Create `cache.go`: persistent TTL dedupe store.
- Create `volume.go`: CRI mount to `Volume` conversion and kubelet filesystem inference.
- Create `watch.go`: callback watch, channel watch, polling loop, optional event acceleration.
- Create `runtime_grpc.go`: production CRI client over unix-domain gRPC.
- Create `*_test.go` files beside the code they exercise.
- Create `README.md`: minimal usage examples and behavior notes.

---

### Task 1: Initialize module and exported API surface

**Files:**
- Create: `go.mod`
- Create: `doc.go`
- Create: `types.go`
- Test: `api_test.go`

- [ ] **Step 1: Create the Go module file**

Create `go.mod`:

```go
module github.com/yckao/crio-discover

go 1.24.0

require (
	google.golang.org/grpc v1.72.1
	k8s.io/cri-api v0.34.8
)
```

- [ ] **Step 2: Write the failing public API compile test**

Create `api_test.go`:

```go
package criodiscover

import (
	"context"
	"testing"
	"time"
)

func TestPublicAPISurfaceCompiles(t *testing.T) {
	var _ Discoverer = (*fakeDiscovererForAPITest)(nil)

	cfg := DefaultConfig()
	cfg.CRISocketPath = "/var/run/crio/crio.sock"
	cfg.CachePath = "/tmp/crio-discover-cache.json"
	cfg.NotificationTTL = time.Minute
	cfg.Predicates = []Predicate{func(c Container) bool { return c.ID != "" }}
	cfg.ErrorHandler = func(error) {}

	if cfg.CRISocketPath == "" {
		t.Fatal("default config must have a CRI socket path")
	}
}

type fakeDiscovererForAPITest struct{}

func (*fakeDiscovererForAPITest) List(context.Context) ([]Container, error) { return nil, nil }
func (*fakeDiscovererForAPITest) Watch(context.Context, Handler) error      { return nil }
func (*fakeDiscovererForAPITest) WatchChan(context.Context) (<-chan Container, <-chan error) {
	containers := make(chan Container)
	errs := make(chan error)
	close(containers)
	close(errs)
	return containers, errs
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run:

```bash
go test ./...
```

Expected: FAIL because `Discoverer`, `Container`, `DefaultConfig`, `Predicate`, and `Handler` are not defined.

- [ ] **Step 4: Add package documentation**

Create `doc.go`:

```go
// Package criodiscover discovers running CRI-O containers through the
// Kubernetes CRI runtime service on a local unix socket.
//
// It provides one-time listing and continuous watching APIs. Continuous
// watching uses polling as the reliability baseline and can use CRI container
// events to reduce notification latency when the runtime supports them.
package criodiscover
```

- [ ] **Step 5: Add exported types**

Create `types.go`:

```go
package criodiscover

import (
	"context"
	"time"
)

// Discoverer is the public interface implemented by this package.
// Users can mock this interface in their own tests.
type Discoverer interface {
	List(ctx context.Context) ([]Container, error)
	Watch(ctx context.Context, handler Handler) error
	WatchChan(ctx context.Context) (<-chan Container, <-chan error)
}

// Handler receives deduplicated container discoveries from Watch.
type Handler func(context.Context, Container) error

// ErrorHandler receives recoverable watch errors while discovery continues.
type ErrorHandler func(error)

// Predicate returns true when a discovered container should be included.
type Predicate func(Container) bool

// Config controls CRI-O discovery behavior.
type Config struct {
	CRISocketPath   string
	KubeletRoot     string
	PollInterval    time.Duration
	EnableEvents    bool
	NotificationTTL time.Duration
	CachePath       string
	Predicates      []Predicate
	ErrorHandler    ErrorHandler
}

// ContainerState is a normalized CRI container state.
type ContainerState string

const (
	ContainerStateCreated ContainerState = "created"
	ContainerStateRunning ContainerState = "running"
	ContainerStateExited  ContainerState = "exited"
	ContainerStateUnknown ContainerState = "unknown"
)

// VolumeType is a best-effort Kubernetes volume source classification.
type VolumeType string

const (
	VolumeTypeHostPath              VolumeType = "hostPath"
	VolumeTypeEmptyDir              VolumeType = "emptyDir"
	VolumeTypeSecret                VolumeType = "secret"
	VolumeTypeConfigMap             VolumeType = "configMap"
	VolumeTypeProjected             VolumeType = "projected"
	VolumeTypePersistentVolumeClaim VolumeType = "persistentVolumeClaim"
	VolumeTypeDownwardAPI           VolumeType = "downwardAPI"
	VolumeTypeUnknown               VolumeType = "unknown"
)

// Container is normalized metadata for a running CRI container.
type Container struct {
	ID          string
	PodID       string
	Name        string
	Image       string
	ImageRef    string
	State       ContainerState
	CreatedAt   time.Time
	Labels      map[string]string
	Annotations map[string]string
	Kubernetes  KubernetesMetadata
	Runtime     RuntimeMetadata
	Volumes     []Volume
}

// KubernetesMetadata contains common Kubernetes identity fields parsed from CRI labels.
type KubernetesMetadata struct {
	Namespace     string
	PodName       string
	PodUID        string
	ContainerName string
	SandboxID     string
	Attempt       uint32
}

// RuntimeMetadata contains runtime-specific fields that are useful to callers.
type RuntimeMetadata struct {
	RuntimeHandler string
	ImageID        string
	RawLabels      map[string]string
	RawAnnotations map[string]string
}

// Volume describes a container mount with best-effort source inference.
type Volume struct {
	HostPath      string
	ContainerPath string
	ReadOnly      bool
	Propagation   string
	Type          VolumeType
	Name          string
	Source        string
}
```

- [ ] **Step 6: Add config defaults used by the compile test**

Create `options.go`:

```go
package criodiscover

import "time"

// Option mutates Config before validation in New.
type Option func(*Config)

// DefaultConfig returns production-oriented defaults for CRI-O on Kubernetes nodes.
func DefaultConfig() Config {
	return Config{
		CRISocketPath: "/var/run/crio/crio.sock",
		KubeletRoot:   "/var/lib/kubelet",
		PollInterval:  30 * time.Second,
		EnableEvents:  true,
	}
}
```

- [ ] **Step 7: Run the test to verify it passes**

Run:

```bash
go test ./...
```

Expected: PASS.

- [ ] **Step 8: Commit**

Run:

```bash
git init
git add go.mod doc.go types.go options.go api_test.go
git commit -m "feat: define public discovery api"
```

Expected: commit succeeds. If git user identity is not configured, run `git status --short` and keep the files staged for the next worker.

---

### Task 2: Add functional options and config validation

**Files:**
- Modify: `options.go`
- Test: `options_test.go`

- [ ] **Step 1: Write failing option and validation tests**

Create `options_test.go`:

```go
package criodiscover

import (
	"strings"
	"testing"
	"time"
)

func TestOptionsApplyToConfig(t *testing.T) {
	cfg := DefaultConfig()
	err := applyOptions(&cfg,
		WithPollInterval(5*time.Second),
		WithEvents(false),
		WithCache("/tmp/cache.json", time.Hour),
		WithPredicate(func(Container) bool { return true }),
		WithPredicates(func(Container) bool { return true }),
		WithErrorHandler(func(error) {}),
	)
	if err != nil {
		t.Fatalf("apply options: %v", err)
	}
	if cfg.PollInterval != 5*time.Second {
		t.Fatalf("poll interval = %s", cfg.PollInterval)
	}
	if cfg.EnableEvents {
		t.Fatal("events should be disabled")
	}
	if cfg.CachePath != "/tmp/cache.json" || cfg.NotificationTTL != time.Hour {
		t.Fatalf("cache config = %q %s", cfg.CachePath, cfg.NotificationTTL)
	}
	if len(cfg.Predicates) != 2 {
		t.Fatalf("predicates = %d", len(cfg.Predicates))
	}
	if cfg.ErrorHandler == nil {
		t.Fatal("error handler should be set")
	}
}

func TestValidateConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
		want string
	}{
		{name: "empty socket", edit: func(c *Config) { c.CRISocketPath = "" }, want: "CRI socket path"},
		{name: "empty kubelet root", edit: func(c *Config) { c.KubeletRoot = "" }, want: "kubelet root"},
		{name: "bad poll interval", edit: func(c *Config) { c.PollInterval = 0 }, want: "poll interval"},
		{name: "negative ttl", edit: func(c *Config) { c.NotificationTTL = -time.Second }, want: "notification TTL"},
		{name: "ttl without path", edit: func(c *Config) { c.NotificationTTL = time.Minute; c.CachePath = "" }, want: "cache path"},
		{name: "nil predicate", edit: func(c *Config) { c.Predicates = []Predicate{nil} }, want: "predicate 0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.edit(&cfg)
			err := validateConfig(cfg)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %q does not contain %q", err.Error(), tt.want)
			}
		})
	}
}

func TestApplyOptionsRejectsNilOption(t *testing.T) {
	cfg := DefaultConfig()
	err := applyOptions(&cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "option 0") {
		t.Fatalf("expected option error, got %v", err)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```bash
go test ./...
```

Expected: FAIL because option helpers and validation helpers are missing.

- [ ] **Step 3: Replace `options.go` with full option implementation**

Replace `options.go`:

```go
package criodiscover

import (
	"fmt"
	"time"
)

// Option mutates Config before validation in New.
type Option func(*Config)

// DefaultConfig returns production-oriented defaults for CRI-O on Kubernetes nodes.
func DefaultConfig() Config {
	return Config{
		CRISocketPath: "/var/run/crio/crio.sock",
		KubeletRoot:   "/var/lib/kubelet",
		PollInterval:  30 * time.Second,
		EnableEvents:  true,
	}
}

// WithPredicate appends one predicate.
func WithPredicate(p Predicate) Option {
	return func(c *Config) {
		c.Predicates = append(c.Predicates, p)
	}
}

// WithPredicates appends multiple predicates.
func WithPredicates(predicates ...Predicate) Option {
	return func(c *Config) {
		c.Predicates = append(c.Predicates, predicates...)
	}
}

// WithPollInterval sets the polling interval used by Watch and WatchChan.
func WithPollInterval(d time.Duration) Option {
	return func(c *Config) {
		c.PollInterval = d
	}
}

// WithEvents enables or disables CRI event acceleration.
func WithEvents(enabled bool) Option {
	return func(c *Config) {
		c.EnableEvents = enabled
	}
}

// WithCache configures persistent notification dedupe.
func WithCache(path string, ttl time.Duration) Option {
	return func(c *Config) {
		c.CachePath = path
		c.NotificationTTL = ttl
	}
}

// WithErrorHandler configures callback-watch reporting for recoverable errors.
func WithErrorHandler(h ErrorHandler) Option {
	return func(c *Config) {
		c.ErrorHandler = h
	}
}

func applyOptions(config *Config, opts ...Option) error {
	for i, opt := range opts {
		if opt == nil {
			return fmt.Errorf("option %d is nil", i)
		}
		opt(config)
	}
	return validateConfig(*config)
}

func validateConfig(config Config) error {
	if config.CRISocketPath == "" {
		return fmt.Errorf("CRI socket path is required")
	}
	if config.KubeletRoot == "" {
		return fmt.Errorf("kubelet root is required")
	}
	if config.PollInterval <= 0 {
		return fmt.Errorf("poll interval must be positive")
	}
	if config.NotificationTTL < 0 {
		return fmt.Errorf("notification TTL must not be negative")
	}
	if config.NotificationTTL > 0 && config.CachePath == "" {
		return fmt.Errorf("cache path is required when notification TTL is positive")
	}
	for i, predicate := range config.Predicates {
		if predicate == nil {
			return fmt.Errorf("predicate %d is nil", i)
		}
	}
	return nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run:

```bash
go test ./...
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add options.go options_test.go
git commit -m "feat: add discovery config options"
```

Expected: commit succeeds or files remain staged if git identity is missing.

---

### Task 3: Add internal runtime model and metadata normalization

**Files:**
- Create: `runtime_client.go`
- Create: `discoverer.go`
- Test: `discoverer_test.go`

- [ ] **Step 1: Write failing metadata and predicate tests**

Create `discoverer_test.go`:

```go
package criodiscover

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
			"io.kubernetes.pod.namespace":    "default",
			"io.kubernetes.pod.name":         "demo",
			"io.kubernetes.pod.uid":          "pod-uid",
			"io.kubernetes.container.name":   "app",
			"io.kubernetes.sandbox.id":       "sandbox-id",
			"custom":                         "value",
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
go test ./...
```

Expected: FAIL because `discoverer`, `runtimeContainer`, `runtimeMount`, `New`, and methods are missing.

- [ ] **Step 3: Add internal runtime abstractions**

Create `runtime_client.go`:

```go
package criodiscover

import "context"

type runtimeClient interface {
	ListContainers(ctx context.Context) ([]runtimeContainer, error)
	ContainerStatus(ctx context.Context, id string) (runtimeContainer, error)
	WatchEvents(ctx context.Context) (runtimeEventStream, error)
	Close() error
}

type runtimeEventStream interface {
	Recv() (runtimeEvent, error)
}

type runtimeContainer struct {
	ID             string
	PodSandboxID   string
	Name           string
	Attempt        uint32
	Image          string
	ImageRef       string
	ImageID        string
	RuntimeHandler string
	State          ContainerState
	CreatedAt      time.Time
	Labels         map[string]string
	Annotations    map[string]string
	Mounts         []runtimeMount
}

type runtimeMount struct {
	HostPath      string
	ContainerPath string
	ReadOnly      bool
	Propagation   string
}

type runtimeEventType string

const (
	runtimeEventCreated runtimeEventType = "created"
	runtimeEventStarted runtimeEventType = "started"
	runtimeEventStopped runtimeEventType = "stopped"
	runtimeEventDeleted runtimeEventType = "deleted"
	runtimeEventUnknown runtimeEventType = "unknown"
)

type runtimeEvent struct {
	ContainerID string
	Type        runtimeEventType
}
```

Add the missing import at the top of `runtime_client.go` by replacing the import line with:

```go
import (
	"context"
	"time"
)
```

- [ ] **Step 4: Add constructor, normalization, and initial List skeleton**

Create `discoverer.go`:

```go
package criodiscover

import (
	"context"
	"errors"
	"fmt"
)

type volumeResolver interface {
	Resolve(podUID string, mounts []runtimeMount) []Volume
}

type discoverer struct {
	config   Config
	client   runtimeClient
	resolver volumeResolver
}

// New validates config, applies options, and returns the default Discoverer implementation.
func New(config Config, opts ...Option) (Discoverer, error) {
	if err := applyOptions(&config, opts...); err != nil {
		return nil, err
	}
	client, err := newGRPCRuntimeClient(config.CRISocketPath)
	if err != nil {
		return nil, err
	}
	return &discoverer{
		config:   config,
		client:   client,
		resolver: newKubeletVolumeResolver(config.KubeletRoot),
	}, nil
}

func (d *discoverer) List(ctx context.Context) ([]Container, error) {
	if d.client == nil {
		return nil, errors.New("runtime client is nil")
	}
	containers, err := d.client.ListContainers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list containers: %w", err)
	}
	result := make([]Container, 0, len(containers))
	for _, candidate := range containers {
		if candidate.State != ContainerStateRunning {
			continue
		}
		status, err := d.client.ContainerStatus(ctx, candidate.ID)
		if err != nil {
			return nil, fmt.Errorf("container status %s: %w", candidate.ID, err)
		}
		if status.State != ContainerStateRunning {
			continue
		}
		if status.PodSandboxID == "" {
			status.PodSandboxID = candidate.PodSandboxID
		}
		container := d.containerFromRuntime(status)
		if d.matches(container) {
			result = append(result, container)
		}
	}
	return result, nil
}

func (d *discoverer) containerFromRuntime(rc runtimeContainer) Container {
	labels := cloneStringMap(rc.Labels)
	annotations := cloneStringMap(rc.Annotations)
	kubernetes := kubernetesMetadataFromRuntime(rc, labels)
	resolver := d.resolver
	if resolver == nil {
		resolver = newKubeletVolumeResolver(d.config.KubeletRoot)
	}
	return Container{
		ID:          rc.ID,
		PodID:       rc.PodSandboxID,
		Name:        rc.Name,
		Image:       rc.Image,
		ImageRef:    rc.ImageRef,
		State:       rc.State,
		CreatedAt:   rc.CreatedAt,
		Labels:      labels,
		Annotations: annotations,
		Kubernetes:  kubernetes,
		Runtime: RuntimeMetadata{
			RuntimeHandler: rc.RuntimeHandler,
			ImageID:        rc.ImageID,
			RawLabels:      cloneStringMap(rc.Labels),
			RawAnnotations: cloneStringMap(rc.Annotations),
		},
		Volumes: resolver.Resolve(kubernetes.PodUID, rc.Mounts),
	}
}

func kubernetesMetadataFromRuntime(rc runtimeContainer, labels map[string]string) KubernetesMetadata {
	containerName := labels["io.kubernetes.container.name"]
	if containerName == "" {
		containerName = rc.Name
	}
	sandboxID := labels["io.kubernetes.sandbox.id"]
	if sandboxID == "" {
		sandboxID = rc.PodSandboxID
	}
	return KubernetesMetadata{
		Namespace:     labels["io.kubernetes.pod.namespace"],
		PodName:       labels["io.kubernetes.pod.name"],
		PodUID:        labels["io.kubernetes.pod.uid"],
		ContainerName: containerName,
		SandboxID:     sandboxID,
		Attempt:       rc.Attempt,
	}
}

func (d *discoverer) matches(container Container) bool {
	for _, predicate := range d.config.Predicates {
		if !predicate(container) {
			return false
		}
	}
	return true
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
```

- [ ] **Step 5: Add compile stubs for volume resolver and CRI client**

Create `volume.go`:

```go
package criodiscover

type kubeletVolumeResolver struct {
	root string
}

func newKubeletVolumeResolver(root string) *kubeletVolumeResolver {
	return &kubeletVolumeResolver{root: root}
}

func (r *kubeletVolumeResolver) Resolve(string, []runtimeMount) []Volume {
	return nil
}
```

Create `runtime_grpc.go`:

```go
package criodiscover

import "context"

type grpcRuntimeClient struct{}

func newGRPCRuntimeClient(string) (runtimeClient, error) {
	return &grpcRuntimeClient{}, nil
}

func (*grpcRuntimeClient) ListContainers(context.Context) ([]runtimeContainer, error) {
	return nil, nil
}

func (*grpcRuntimeClient) ContainerStatus(context.Context, string) (runtimeContainer, error) {
	return runtimeContainer{}, nil
}

func (*grpcRuntimeClient) WatchEvents(context.Context) (runtimeEventStream, error) {
	return nil, errEventsUnsupported
}

func (*grpcRuntimeClient) Close() error { return nil }
```

Create `watch.go`:

```go
package criodiscover

import (
	"context"
	"errors"
)

var errEventsUnsupported = errors.New("CRI container events unsupported")

func (d *discoverer) Watch(context.Context, Handler) error {
	return errors.New("watch is not implemented")
}

func (d *discoverer) WatchChan(context.Context) (<-chan Container, <-chan error) {
	containers := make(chan Container)
	errs := make(chan error)
	close(containers)
	close(errs)
	return containers, errs
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run:

```bash
go test ./...
```

Expected: PASS.

- [ ] **Step 7: Commit**

Run:

```bash
git add runtime_client.go discoverer.go volume.go runtime_grpc.go watch.go discoverer_test.go
git commit -m "feat: normalize cri container metadata"
```

Expected: commit succeeds or files remain staged if git identity is missing.

---

### Task 4: Implement kubelet volume inference

**Files:**
- Modify: `volume.go`
- Test: `volume_test.go`

- [ ] **Step 1: Write failing volume resolver tests**

Create `volume_test.go`:

```go
package criodiscover

import (
	"path/filepath"
	"testing"
)

func TestVolumeResolverInfersKnownKubeletVolumeTypes(t *testing.T) {
	root := t.TempDir()
	podUID := "pod-uid"
	tests := []struct {
		plugin string
		name   string
		want   VolumeType
	}{
		{plugin: "kubernetes.io~host-path", name: "host", want: VolumeTypeHostPath},
		{plugin: "kubernetes.io~empty-dir", name: "cache", want: VolumeTypeEmptyDir},
		{plugin: "kubernetes.io~secret", name: "secret", want: VolumeTypeSecret},
		{plugin: "kubernetes.io~configmap", name: "config", want: VolumeTypeConfigMap},
		{plugin: "kubernetes.io~projected", name: "projected", want: VolumeTypeProjected},
		{plugin: "kubernetes.io~downward-api", name: "downward", want: VolumeTypeDownwardAPI},
		{plugin: "kubernetes.io~csi", name: "pvc", want: VolumeTypePersistentVolumeClaim},
	}

	resolver := newKubeletVolumeResolver(root)
	for _, tt := range tests {
		t.Run(tt.plugin, func(t *testing.T) {
			hostPath := filepath.Join(root, "pods", podUID, "volumes", tt.plugin, tt.name)
			volumes := resolver.Resolve(podUID, []runtimeMount{{HostPath: hostPath, ContainerPath: "/mnt", ReadOnly: true, Propagation: "PROPAGATION_PRIVATE"}})
			if len(volumes) != 1 {
				t.Fatalf("volumes = %d", len(volumes))
			}
			got := volumes[0]
			if got.Type != tt.want || got.Name != tt.name || got.Source != tt.plugin {
				t.Fatalf("volume = %#v", got)
			}
			if got.HostPath != hostPath || got.ContainerPath != "/mnt" || !got.ReadOnly || got.Propagation != "PROPAGATION_PRIVATE" {
				t.Fatalf("mount fields = %#v", got)
			}
		})
	}
}

func TestVolumeResolverHandlesVolumeSubpaths(t *testing.T) {
	root := t.TempDir()
	podUID := "pod-uid"
	resolver := newKubeletVolumeResolver(root)
	hostPath := filepath.Join(root, "pods", podUID, "volume-subpaths", "config", "app", "0")
	volumes := resolver.Resolve(podUID, []runtimeMount{{HostPath: hostPath, ContainerPath: "/etc/config"}})
	if len(volumes) != 1 {
		t.Fatalf("volumes = %d", len(volumes))
	}
	if volumes[0].Name != "config" || volumes[0].Type != VolumeTypeUnknown || volumes[0].Source != "volume-subpaths" {
		t.Fatalf("volume = %#v", volumes[0])
	}
}

func TestVolumeResolverFallsBackToUnknown(t *testing.T) {
	resolver := newKubeletVolumeResolver("/var/lib/kubelet")
	volumes := resolver.Resolve("", []runtimeMount{{HostPath: "/opt/data", ContainerPath: "/data"}})
	if len(volumes) != 1 {
		t.Fatalf("volumes = %d", len(volumes))
	}
	if volumes[0].Type != VolumeTypeUnknown || volumes[0].Name != "" || volumes[0].Source != "" {
		t.Fatalf("volume = %#v", volumes[0])
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
go test ./...
```

Expected: FAIL because `Resolve` returns nil.

- [ ] **Step 3: Replace `volume.go` with inference implementation**

Replace `volume.go`:

```go
package criodiscover

import (
	"path/filepath"
	"strings"
)

type kubeletVolumeResolver struct {
	root string
}

func newKubeletVolumeResolver(root string) *kubeletVolumeResolver {
	return &kubeletVolumeResolver{root: filepath.Clean(root)}
}

func (r *kubeletVolumeResolver) Resolve(podUID string, mounts []runtimeMount) []Volume {
	volumes := make([]Volume, 0, len(mounts))
	for _, mount := range mounts {
		volumeType, name, source := r.infer(podUID, mount.HostPath)
		volumes = append(volumes, Volume{
			HostPath:      mount.HostPath,
			ContainerPath: mount.ContainerPath,
			ReadOnly:      mount.ReadOnly,
			Propagation:   mount.Propagation,
			Type:          volumeType,
			Name:          name,
			Source:        source,
		})
	}
	return volumes
}

func (r *kubeletVolumeResolver) infer(podUID, hostPath string) (VolumeType, string, string) {
	if podUID == "" || hostPath == "" || r.root == "" {
		return VolumeTypeUnknown, "", ""
	}
	cleanHostPath := filepath.Clean(hostPath)
	volumesPrefix := filepath.Join(r.root, "pods", podUID, "volumes") + string(filepath.Separator)
	if strings.HasPrefix(cleanHostPath, volumesPrefix) {
		rel, err := filepath.Rel(volumesPrefix, cleanHostPath)
		if err != nil {
			return VolumeTypeUnknown, "", ""
		}
		parts := strings.Split(rel, string(filepath.Separator))
		if len(parts) >= 2 {
			plugin := parts[0]
			name := parts[1]
			return volumeTypeForPlugin(plugin), name, plugin
		}
	}

	subpathsPrefix := filepath.Join(r.root, "pods", podUID, "volume-subpaths") + string(filepath.Separator)
	if strings.HasPrefix(cleanHostPath, subpathsPrefix) {
		rel, err := filepath.Rel(subpathsPrefix, cleanHostPath)
		if err != nil {
			return VolumeTypeUnknown, "", ""
		}
		parts := strings.Split(rel, string(filepath.Separator))
		if len(parts) >= 1 && parts[0] != "" {
			return VolumeTypeUnknown, parts[0], "volume-subpaths"
		}
	}

	return VolumeTypeUnknown, "", ""
}

func volumeTypeForPlugin(plugin string) VolumeType {
	switch plugin {
	case "kubernetes.io~host-path":
		return VolumeTypeHostPath
	case "kubernetes.io~empty-dir":
		return VolumeTypeEmptyDir
	case "kubernetes.io~secret":
		return VolumeTypeSecret
	case "kubernetes.io~configmap":
		return VolumeTypeConfigMap
	case "kubernetes.io~projected":
		return VolumeTypeProjected
	case "kubernetes.io~downward-api":
		return VolumeTypeDownwardAPI
	case "kubernetes.io~csi", "kubernetes.io~aws-ebs", "kubernetes.io~gce-pd", "kubernetes.io~azure-disk", "kubernetes.io~azure-file", "kubernetes.io~nfs", "kubernetes.io~rbd", "kubernetes.io~cephfs":
		return VolumeTypePersistentVolumeClaim
	default:
		return VolumeTypeUnknown
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
go test ./...
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add volume.go volume_test.go
git commit -m "feat: infer kubelet volume metadata"
```

Expected: commit succeeds or files remain staged if git identity is missing.

---

### Task 5: Implement persistent TTL notification cache

**Files:**
- Create: `cache.go`
- Test: `cache_test.go`

- [ ] **Step 1: Write failing cache tests**

Create `cache_test.go`:

```go
package criodiscover

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNoopCacheNeverSuppresses(t *testing.T) {
	cache := newNoopDedupeStore()
	if cache.Seen("container") {
		t.Fatal("noop cache should not suppress")
	}
	if err := cache.Mark("container"); err != nil {
		t.Fatalf("mark noop: %v", err)
	}
	if cache.Seen("container") {
		t.Fatal("noop cache should still not suppress")
	}
}

func TestFileCachePersistsAndExpires(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.json")
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	cache, err := newFileDedupeStore(path, time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	if cache.Seen("abc") {
		t.Fatal("new entry should not be seen")
	}
	if err := cache.Mark("abc"); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if !cache.Seen("abc") {
		t.Fatal("entry should be seen after mark")
	}

	now = now.Add(30 * time.Minute)
	reloaded, err := newFileDedupeStore(path, time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatalf("reload cache: %v", err)
	}
	if !reloaded.Seen("abc") {
		t.Fatal("entry should survive reload")
	}

	now = now.Add(31 * time.Minute)
	if reloaded.Seen("abc") {
		t.Fatal("entry should expire")
	}
}

func TestFileCacheRejectsCorruptJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.json")
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatalf("write corrupt cache: %v", err)
	}
	_, err := newFileDedupeStore(path, time.Minute, time.Now)
	if err == nil || !strings.Contains(err.Error(), "decode cache") {
		t.Fatalf("expected decode error, got %v", err)
	}
}

func TestFileCacheWritesVersionedJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.json")
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	cache, err := newFileDedupeStore(path, time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatalf("new cache: %v", err)
	}
	if err := cache.Mark("abc"); err != nil {
		t.Fatalf("mark: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read cache: %v", err)
	}
	var payload fileCachePayload
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("unmarshal cache: %v", err)
	}
	if payload.Version != 1 {
		t.Fatalf("version = %d", payload.Version)
	}
	entry := payload.Entries["abc"]
	if !entry.LastNotifiedAt.Equal(now) || !entry.ExpiresAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("entry = %#v", entry)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
go test ./...
```

Expected: FAIL because cache types and constructors are missing.

- [ ] **Step 3: Add cache implementation**

Create `cache.go`:

```go
package criodiscover

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type dedupeStore interface {
	Seen(id string) bool
	Mark(id string) error
	Close() error
}

type noopDedupeStore struct{}

func newNoopDedupeStore() dedupeStore { return noopDedupeStore{} }
func (noopDedupeStore) Seen(string) bool { return false }
func (noopDedupeStore) Mark(string) error { return nil }
func (noopDedupeStore) Close() error { return nil }

type fileCachePayload struct {
	Version int                       `json:"version"`
	Entries map[string]fileCacheEntry `json:"entries"`
}

type fileCacheEntry struct {
	LastNotifiedAt time.Time `json:"last_notified_at"`
	ExpiresAt      time.Time `json:"expires_at"`
}

type fileDedupeStore struct {
	path    string
	ttl     time.Duration
	now     func() time.Time
	mu      sync.Mutex
	entries map[string]fileCacheEntry
}

func newDedupeStore(config Config) (dedupeStore, error) {
	if config.NotificationTTL <= 0 {
		return newNoopDedupeStore(), nil
	}
	return newFileDedupeStore(config.CachePath, config.NotificationTTL, time.Now)
}

func newFileDedupeStore(path string, ttl time.Duration, now func() time.Time) (*fileDedupeStore, error) {
	if now == nil {
		now = time.Now
	}
	store := &fileDedupeStore{
		path:    path,
		ttl:     ttl,
		now:     now,
		entries: map[string]fileCacheEntry{},
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return store, nil
		}
		return nil, fmt.Errorf("read cache %s: %w", path, err)
	}
	var payload fileCachePayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return nil, fmt.Errorf("decode cache %s: %w", path, err)
	}
	if payload.Version != 1 {
		return nil, fmt.Errorf("decode cache %s: unsupported version %d", path, payload.Version)
	}
	if payload.Entries != nil {
		store.entries = payload.Entries
	}
	store.pruneLocked(store.now())
	return store, nil
}

func (s *fileDedupeStore) Seen(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.pruneLocked(now)
	entry, ok := s.entries[id]
	return ok && entry.ExpiresAt.After(now)
}

func (s *fileDedupeStore) Mark(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.entries[id] = fileCacheEntry{LastNotifiedAt: now, ExpiresAt: now.Add(s.ttl)}
	s.pruneLocked(now)
	return s.saveLocked()
}

func (s *fileDedupeStore) Close() error { return nil }

func (s *fileDedupeStore) pruneLocked(now time.Time) {
	for id, entry := range s.entries {
		if !entry.ExpiresAt.After(now) {
			delete(s.entries, id)
		}
	}
}

func (s *fileDedupeStore) saveLocked() error {
	payload := fileCachePayload{Version: 1, Entries: s.entries}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return fmt.Errorf("encode cache %s: %w", s.path, err)
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("create cache directory %s: %w", filepath.Dir(s.path), err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write cache temp file %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("replace cache file %s: %w", s.path, err)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
go test ./...
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add cache.go cache_test.go
git commit -m "feat: add persistent notification dedupe cache"
```

Expected: commit succeeds or files remain staged if git identity is missing.

---

### Task 6: Complete List with fake runtime client tests

**Files:**
- Modify: `discoverer_test.go`
- Test helper in: `fake_runtime_test.go`

- [ ] **Step 1: Add fake runtime client test helper**

Create `fake_runtime_test.go`:

```go
package criodiscover

import (
	"context"
	"errors"
	"io"
	"sync"
)

type fakeRuntimeClient struct {
	mu          sync.Mutex
	listed       []runtimeContainer
	statuses     map[string]runtimeContainer
	listErr      error
	listErrAfter int
	listCalls    int
	statusErr    map[string]error
	eventStream runtimeEventStream
	eventErr    error
	closed      bool
}

func (f *fakeRuntimeClient) ListContainers(context.Context) ([]runtimeContainer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listCalls++
	if f.listErr != nil && (f.listErrAfter == 0 || f.listCalls > f.listErrAfter) {
		return nil, f.listErr
	}
	out := append([]runtimeContainer(nil), f.listed...)
	return out, nil
}

func (f *fakeRuntimeClient) ContainerStatus(_ context.Context, id string) (runtimeContainer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.statusErr[id]; err != nil {
		return runtimeContainer{}, err
	}
	status, ok := f.statuses[id]
	if !ok {
		return runtimeContainer{}, errors.New("status not found")
	}
	return status, nil
}

func (f *fakeRuntimeClient) WatchEvents(context.Context) (runtimeEventStream, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.eventErr != nil {
		return nil, f.eventErr
	}
	if f.eventStream == nil {
		return &fakeRuntimeEventStream{events: nil}, nil
	}
	return f.eventStream, nil
}

func (f *fakeRuntimeClient) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

type fakeRuntimeEventStream struct {
	events []runtimeEvent
	index  int
	err    error
}

func (s *fakeRuntimeEventStream) Recv() (runtimeEvent, error) {
	if s.index < len(s.events) {
		event := s.events[s.index]
		s.index++
		return event, nil
	}
	if s.err != nil {
		return runtimeEvent{}, s.err
	}
	return runtimeEvent{}, io.EOF
}
```

- [ ] **Step 2: Add List behavior tests**

Append to `discoverer_test.go`:

```go
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
		config: Config{Predicates: []Predicate{func(c Container) bool { return c.Kubernetes.Namespace == "default" }}},
		client: client,
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
```

Add these imports to `discoverer_test.go`:

```go
	"errors"
	"strings"
```

- [ ] **Step 3: Run tests to verify they pass**

Run:

```bash
go test ./...
```

Expected: PASS because `List` was implemented in Task 3.

- [ ] **Step 4: Commit**

Run:

```bash
git add fake_runtime_test.go discoverer_test.go
git commit -m "test: cover list discovery with fake runtime"
```

Expected: commit succeeds or files remain staged if git identity is missing.

---

### Task 7: Implement callback Watch with startup scan, polling, dedupe, and error handler

**Files:**
- Modify: `watch.go`
- Test: `watch_test.go`

- [ ] **Step 1: Write failing watch tests**

Create `watch_test.go`:

```go
package criodiscover

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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
go test ./...
```

Expected: FAIL because `Watch` returns the skeleton error.

- [ ] **Step 3: Replace `watch.go` with watch implementation**

Replace `watch.go`:

```go
package criodiscover

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
			if err := d.handleContainerID(ctx, id, cache, handler, report); err != nil {
				return err
			}
		}
	}
}

func (d *discoverer) scanAndHandle(ctx context.Context, cache dedupeStore, handler Handler, report func(error), fatalListError bool) error {
	containers, err := d.client.ListContainers(ctx)
	if err != nil {
		wrapped := fmt.Errorf("list containers: %w", err)
		if fatalListError {
			return wrapped
		}
		report(wrapped)
		return nil
	}
	for _, candidate := range containers {
		if candidate.State != ContainerStateRunning {
			continue
		}
		if err := d.handleContainerID(ctx, candidate.ID, cache, handler, report); err != nil {
			return err
		}
	}
	return nil
}

func (d *discoverer) handleContainerID(ctx context.Context, id string, cache dedupeStore, handler Handler, report func(error)) error {
	status, err := d.client.ContainerStatus(ctx, id)
	if err != nil {
		report(fmt.Errorf("container status %s: %w", id, err))
		return nil
	}
	if status.State != ContainerStateRunning {
		return nil
	}
	container := d.containerFromRuntime(status)
	if !d.matches(container) {
		return nil
	}
	if cache.Seen(container.ID) {
		return nil
	}
	if err := handler(ctx, container); err != nil {
		return err
	}
	if err := cache.Mark(container.ID); err != nil {
		report(fmt.Errorf("mark notification cache %s: %w", container.ID, err))
	}
	return nil
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
go test ./...
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add watch.go watch_test.go
git commit -m "feat: watch containers with polling and dedupe"
```

Expected: commit succeeds or files remain staged if git identity is missing.

---

### Task 8: Cover WatchChan and event acceleration

**Files:**
- Modify: `watch_test.go`

- [ ] **Step 1: Add failing WatchChan and event tests**

Append to `watch_test.go`:

```go
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
	container := <-containers
	if container.ID != "running" {
		t.Fatalf("container = %#v", container)
	}
	cancel()
	for range containers {
	}
	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	default:
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
	client := &fakeRuntimeClient{
		listed: []runtimeContainer{},
		statuses: map[string]runtimeContainer{},
		eventStream: &fakeRuntimeEventStream{err: errors.New("event stream failed")},
	}
	cfg := DefaultConfig()
	cfg.EnableEvents = true
	cfg.PollInterval = 10 * time.Millisecond
	var reported []error
	cfg.ErrorHandler = func(err error) { reported = append(reported, err) }
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := d.Watch(ctx, func(context.Context, Container) error { return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Watch error = %v", err)
	}
	if len(reported) == 0 {
		t.Fatal("expected event error to be reported")
	}
}
```

- [ ] **Step 2: Run tests**

Run:

```bash
go test ./...
```

Expected: PASS if Task 7 implementation handles WatchChan and events correctly. If `TestWatchChanEmitsContainersAndRecoverableErrors` flakes because the error channel is closed before the default select, replace the select block with:

```go
for err := range errs {
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
```

- [ ] **Step 3: Commit**

Run:

```bash
git add watch_test.go
git commit -m "test: cover channel watch and cri event acceleration"
```

Expected: commit succeeds or files remain staged if git identity is missing.

---

### Task 9: Replace gRPC stub with production CRI client

**Files:**
- Modify: `runtime_grpc.go`
- Test: `runtime_grpc_test.go`

- [ ] **Step 1: Write CRI mapping tests**

Create `runtime_grpc_test.go`:

```go
package criodiscover

import (
	"testing"
	"time"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func TestMapContainerState(t *testing.T) {
	tests := []struct {
		in   runtimeapi.ContainerState
		want ContainerState
	}{
		{runtimeapi.ContainerState_CONTAINER_CREATED, ContainerStateCreated},
		{runtimeapi.ContainerState_CONTAINER_RUNNING, ContainerStateRunning},
		{runtimeapi.ContainerState_CONTAINER_EXITED, ContainerStateExited},
		{runtimeapi.ContainerState_CONTAINER_UNKNOWN, ContainerStateUnknown},
	}
	for _, tt := range tests {
		if got := mapContainerState(tt.in); got != tt.want {
			t.Fatalf("state %s = %s", tt.in.String(), got)
		}
	}
}

func TestMapRuntimeStatus(t *testing.T) {
	created := time.Date(2026, 6, 1, 0, 0, 0, 123, time.UTC)
	status := &runtimeapi.ContainerStatus{
		Id:           "id",
		Metadata:     &runtimeapi.ContainerMetadata{Name: "app", Attempt: 3},
		State:        runtimeapi.ContainerState_CONTAINER_RUNNING,
		CreatedAt:    created.UnixNano(),
		Image:        &runtimeapi.ImageSpec{Image: "resolved", UserSpecifiedImage: "user/app:v1", RuntimeHandler: "runc"},
		ImageRef:     "sha256:abc",
		ImageId:      "image-id",
		Labels:       map[string]string{"label": "value"},
		Annotations:  map[string]string{"annotation": "value"},
		Mounts:       []*runtimeapi.Mount{{HostPath: "/host", ContainerPath: "/container", Readonly: true, Propagation: runtimeapi.MountPropagation_PROPAGATION_PRIVATE}},
	}
	got := mapRuntimeStatus(status, "sandbox")
	if got.ID != "id" || got.PodSandboxID != "sandbox" || got.Name != "app" || got.Attempt != 3 {
		t.Fatalf("identity = %#v", got)
	}
	if got.Image != "user/app:v1" || got.ImageRef != "sha256:abc" || got.ImageID != "image-id" || got.RuntimeHandler != "runc" {
		t.Fatalf("image/runtime = %#v", got)
	}
	if !got.CreatedAt.Equal(created) || got.State != ContainerStateRunning {
		t.Fatalf("state/time = %#v", got)
	}
	if len(got.Mounts) != 1 || got.Mounts[0].HostPath != "/host" || got.Mounts[0].Propagation != "PROPAGATION_PRIVATE" {
		t.Fatalf("mounts = %#v", got.Mounts)
	}
}

func TestMapRuntimeEvent(t *testing.T) {
	event := mapRuntimeEvent(&runtimeapi.ContainerEventResponse{ContainerId: "id", ContainerEventType: runtimeapi.ContainerEventType_CONTAINER_STARTED_EVENT})
	if event.ContainerID != "id" || event.Type != runtimeEventStarted {
		t.Fatalf("event = %#v", event)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
go test ./...
```

Expected: FAIL because mapping helpers do not exist.

- [ ] **Step 3: Replace `runtime_grpc.go` with production client**

Replace `runtime_grpc.go`:

```go
package criodiscover

import (
	"context"
	"fmt"
	"time"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type grpcRuntimeClient struct {
	conn   *grpc.ClientConn
	client runtimeapi.RuntimeServiceClient
}

func newGRPCRuntimeClient(socketPath string) (runtimeClient, error) {
	conn, err := grpc.NewClient("unix://"+socketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("create CRI client: %w", err)
	}
	return &grpcRuntimeClient{conn: conn, client: runtimeapi.NewRuntimeServiceClient(conn)}, nil
}

func (c *grpcRuntimeClient) ListContainers(ctx context.Context) ([]runtimeContainer, error) {
	resp, err := c.client.ListContainers(ctx, &runtimeapi.ListContainersRequest{Filter: &runtimeapi.ContainerFilter{State: &runtimeapi.ContainerStateValue{State: runtimeapi.ContainerState_CONTAINER_RUNNING}}})
	if err != nil {
		return nil, err
	}
	containers := make([]runtimeContainer, 0, len(resp.GetContainers()))
	for _, container := range resp.GetContainers() {
		containers = append(containers, mapRuntimeContainer(container))
	}
	return containers, nil
}

func (c *grpcRuntimeClient) ContainerStatus(ctx context.Context, id string) (runtimeContainer, error) {
	resp, err := c.client.ContainerStatus(ctx, &runtimeapi.ContainerStatusRequest{ContainerId: id, Verbose: false})
	if err != nil {
		return runtimeContainer{}, err
	}
	status := resp.GetStatus()
	if status == nil {
		return runtimeContainer{}, fmt.Errorf("container status %s is nil", id)
	}
	return mapRuntimeStatus(status, ""), nil
}

func (c *grpcRuntimeClient) WatchEvents(ctx context.Context) (runtimeEventStream, error) {
	stream, err := c.client.GetContainerEvents(ctx, &runtimeapi.GetEventsRequest{})
	if err != nil {
		return nil, err
	}
	return &grpcRuntimeEventStream{stream: stream}, nil
}

func (c *grpcRuntimeClient) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

type grpcRuntimeEventStream struct {
	stream grpc.ServerStreamingClient[runtimeapi.ContainerEventResponse]
}

func (s *grpcRuntimeEventStream) Recv() (runtimeEvent, error) {
	resp, err := s.stream.Recv()
	if err != nil {
		return runtimeEvent{}, err
	}
	return mapRuntimeEvent(resp), nil
}

func mapRuntimeContainer(container *runtimeapi.Container) runtimeContainer {
	metadata := container.GetMetadata()
	return runtimeContainer{
		ID:           container.GetId(),
		PodSandboxID: container.GetPodSandboxId(),
		Name:         metadata.GetName(),
		Attempt:      metadata.GetAttempt(),
		Image:        imageName(container.GetImage()),
		ImageRef:     container.GetImageRef(),
		ImageID:      container.GetImageId(),
		State:        mapContainerState(container.GetState()),
		CreatedAt:    unixNanoToTime(container.GetCreatedAt()),
		Labels:       container.GetLabels(),
		Annotations:  container.GetAnnotations(),
	}
}

func mapRuntimeStatus(status *runtimeapi.ContainerStatus, sandboxID string) runtimeContainer {
	metadata := status.GetMetadata()
	mounts := make([]runtimeMount, 0, len(status.GetMounts()))
	for _, mount := range status.GetMounts() {
		mounts = append(mounts, runtimeMount{
			HostPath:      mount.GetHostPath(),
			ContainerPath: mount.GetContainerPath(),
			ReadOnly:      mount.GetReadonly(),
			Propagation:   mount.GetPropagation().String(),
		})
	}
	return runtimeContainer{
		ID:             status.GetId(),
		PodSandboxID:   sandboxID,
		Name:           metadata.GetName(),
		Attempt:        metadata.GetAttempt(),
		Image:          imageName(status.GetImage()),
		ImageRef:       status.GetImageRef(),
		ImageID:        status.GetImageId(),
		RuntimeHandler: status.GetImage().GetRuntimeHandler(),
		State:          mapContainerState(status.GetState()),
		CreatedAt:      unixNanoToTime(status.GetCreatedAt()),
		Labels:         status.GetLabels(),
		Annotations:    status.GetAnnotations(),
		Mounts:         mounts,
	}
}

func mapRuntimeEvent(resp *runtimeapi.ContainerEventResponse) runtimeEvent {
	event := runtimeEvent{ContainerID: resp.GetContainerId(), Type: runtimeEventUnknown}
	switch resp.GetContainerEventType() {
	case runtimeapi.ContainerEventType_CONTAINER_CREATED_EVENT:
		event.Type = runtimeEventCreated
	case runtimeapi.ContainerEventType_CONTAINER_STARTED_EVENT:
		event.Type = runtimeEventStarted
	case runtimeapi.ContainerEventType_CONTAINER_STOPPED_EVENT:
		event.Type = runtimeEventStopped
	case runtimeapi.ContainerEventType_CONTAINER_DELETED_EVENT:
		event.Type = runtimeEventDeleted
	}
	return event
}

func mapContainerState(state runtimeapi.ContainerState) ContainerState {
	switch state {
	case runtimeapi.ContainerState_CONTAINER_CREATED:
		return ContainerStateCreated
	case runtimeapi.ContainerState_CONTAINER_RUNNING:
		return ContainerStateRunning
	case runtimeapi.ContainerState_CONTAINER_EXITED:
		return ContainerStateExited
	case runtimeapi.ContainerState_CONTAINER_UNKNOWN:
		return ContainerStateUnknown
	default:
		return ContainerStateUnknown
	}
}

func imageName(image *runtimeapi.ImageSpec) string {
	if image == nil {
		return ""
	}
	if image.GetUserSpecifiedImage() != "" {
		return image.GetUserSpecifiedImage()
	}
	return image.GetImage()
}

func unixNanoToTime(nanos int64) time.Time {
	if nanos == 0 {
		return time.Time{}
	}
	return time.Unix(0, nanos).UTC()
}
```

- [ ] **Step 4: Run gofmt and tests**

Run:

```bash
gofmt -w runtime_grpc.go runtime_grpc_test.go
go test ./...
```

Expected: PASS. `go test` may download module dependencies and update `go.sum`.

- [ ] **Step 5: Commit**

Run:

```bash
git add runtime_grpc.go runtime_grpc_test.go go.sum
git commit -m "feat: add grpc cri runtime client"
```

Expected: commit succeeds or files remain staged if git identity is missing.

---

### Task 10: Add README and final package verification

**Files:**
- Create: `README.md`
- Modify: `docs/superpowers/specs/2026-06-01-crio-discover-design.md` only if implementation intentionally differs from the spec.

- [ ] **Step 1: Write README**

Create `README.md`:

```markdown
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
```

- [ ] **Step 2: Run full formatting and tests**

Run:

```bash
gofmt -w *.go
go test ./...
```

Expected: PASS.

- [ ] **Step 3: Run package documentation check**

Run:

```bash
go test -run TestPublicAPISurfaceCompiles ./...
go doc github.com/yckao/crio-discover.Discoverer
```

Expected: `go test` PASS, and `go doc` prints the `Discoverer` interface with `List`, `Watch`, and `WatchChan`.

- [ ] **Step 4: Verify no accidental marker strings remain**

Run:

```bash
rg -n "T[B]D|T[O]DO|implement [l]ater|fill in [d]etails" . || true
```

Expected: no matches.

- [ ] **Step 5: Commit**

Run:

```bash
git add README.md docs/superpowers/specs/2026-06-01-crio-discover-design.md
git commit -m "docs: add crio discover usage"
```

Expected: commit succeeds or files remain staged if git identity is missing.

---

## Final Verification

Run these commands after all tasks are complete:

```bash
gofmt -w *.go
go test ./...
go vet ./...
rg -n "T[B]D|T[O]DO|implement [l]ater|fill in [d]etails" . || true
git status --short
```

Expected results:

- `go test ./...` passes.
- `go vet ./...` passes.
- Marker-string scan returns no matches.
- `git status --short` shows only intentional uncommitted files, or no output after successful commits.

## Spec Coverage Self-Review

- Go library package: Tasks 1-3 define the module, package docs, exported interface, and constructor.
- Local CRI socket client: Task 9 implements unix-domain gRPC CRI runtime access.
- Running containers before startup: Task 7 startup scan handles already-running containers.
- Polling plus event acceleration: Task 7 implements polling; Task 8 covers event acceleration behavior.
- Persistent TTL notification dedupe: Task 5 implements the JSON file cache; Task 7 wires it into watch notification emission.
- Metadata model: Tasks 1 and 3 define and populate container, Kubernetes, and runtime metadata.
- Volumes with host path, container path, type, and flags: Task 4 implements volume conversion and kubelet path inference.
- Predicate-only filtering: Tasks 2, 3, and 6 validate all-predicates-must-pass behavior.
- `List`, `Watch`, and `WatchChan`: Tasks 3, 7, and 8 implement and test all execution APIs.
- Recoverable callback errors: Task 7 implements optional `ErrorHandler` and tests polling error reporting.
