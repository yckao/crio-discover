package discover

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Discoverer is the public interface implemented by this package.
// Users can mock this interface in their own tests.
type Discoverer interface {
	List(ctx context.Context) ([]Container, error)
	Watch(ctx context.Context, handler Handler) error
	WatchChan(ctx context.Context) (<-chan Container, <-chan error)
	Close() error
}

// Handler receives deduplicated container discoveries from Watch.
type Handler func(context.Context, Container) error

// ErrorHandler receives recoverable watch errors while discovery continues.
type ErrorHandler func(error)

// Predicate returns true when a discovered container should be included.
type Predicate func(Container) bool

// Config controls CRI-O discovery behavior.
type Config struct {
	CRISocketPath        string
	KubeletRoot          string
	PollInterval         time.Duration
	EnableEvents         bool
	NotificationTTL      time.Duration
	CachePath            string
	Predicates           []Predicate
	ErrorHandler         ErrorHandler
	Logger               *slog.Logger
	PrometheusRegisterer prometheus.Registerer
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
