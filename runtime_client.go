package criodiscovery

import (
	"context"
	"time"
)

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
