package discover

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
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
	resp, err := c.client.ContainerStatus(ctx, &runtimeapi.ContainerStatusRequest{ContainerId: id, Verbose: true})
	if err != nil {
		return runtimeContainer{}, err
	}
	status := resp.GetStatus()
	if status == nil {
		return runtimeContainer{}, fmt.Errorf("container status %s is nil", id)
	}
	return mapRuntimeStatus(status, "", resp.GetInfo()), nil
}

func (c *grpcRuntimeClient) WatchEvents(ctx context.Context) (runtimeEventStream, error) {
	stream, err := c.client.GetContainerEvents(ctx, &runtimeapi.GetEventsRequest{})
	if err != nil {
		return nil, mapGRPCEventError(err)
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
		return runtimeEvent{}, mapGRPCEventError(err)
	}
	return mapRuntimeEvent(resp), nil
}

func mapGRPCEventError(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.Unimplemented {
		return errEventsUnsupported
	}
	return err
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

func mapRuntimeStatus(status *runtimeapi.ContainerStatus, sandboxID string, info map[string]string) runtimeContainer {
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
		RootPath:       rootPathFromCRIInfo(info),
		State:          mapContainerState(status.GetState()),
		CreatedAt:      unixNanoToTime(status.GetCreatedAt()),
		Labels:         status.GetLabels(),
		Annotations:    status.GetAnnotations(),
		Mounts:         mounts,
	}
}

func rootPathFromCRIInfo(info map[string]string) string {
	if len(info) == 0 || info["info"] == "" {
		return ""
	}
	var parsed struct {
		RuntimeSpec struct {
			Root struct {
				Path string `json:"path"`
			} `json:"root"`
		} `json:"runtimeSpec"`
	}
	if err := json.Unmarshal([]byte(info["info"]), &parsed); err != nil {
		return ""
	}
	return parsed.RuntimeSpec.Root.Path
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
