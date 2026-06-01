package discover

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		Id:          "id",
		Metadata:    &runtimeapi.ContainerMetadata{Name: "app", Attempt: 3},
		State:       runtimeapi.ContainerState_CONTAINER_RUNNING,
		CreatedAt:   created.UnixNano(),
		Image:       &runtimeapi.ImageSpec{Image: "resolved", UserSpecifiedImage: "user/app:v1", RuntimeHandler: "runc"},
		ImageRef:    "sha256:abc",
		ImageId:     "image-id",
		Labels:      map[string]string{"label": "value"},
		Annotations: map[string]string{"annotation": "value"},
		Mounts:      []*runtimeapi.Mount{{HostPath: "/host", ContainerPath: "/container", Readonly: true, Propagation: runtimeapi.MountPropagation_PROPAGATION_PRIVATE}},
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

func TestMapRuntimeStatusHandlesCRIO124ShapedStatus(t *testing.T) {
	created := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	status := &runtimeapi.ContainerStatus{
		Id:        "id",
		Metadata:  &runtimeapi.ContainerMetadata{Name: "app"},
		State:     runtimeapi.ContainerState_CONTAINER_RUNNING,
		CreatedAt: created.UnixNano(),
		Image:     &runtimeapi.ImageSpec{Image: "registry/app:v1"},
		ImageRef:  "sha256:abc",
		Labels: map[string]string{
			"io.kubernetes.pod.namespace": "default",
			"io.kubernetes.pod.uid":       "pod-uid",
		},
		Mounts: []*runtimeapi.Mount{{HostPath: "/host", ContainerPath: "/container", Readonly: true}},
	}

	got := mapRuntimeStatus(status, "sandbox")
	if got.Image != "registry/app:v1" {
		t.Fatalf("Image = %q, want registry/app:v1", got.Image)
	}
	if got.ImageID != "" {
		t.Fatalf("ImageID = %q, want empty for CRI-O 1.24-shaped status", got.ImageID)
	}
	if got.RuntimeHandler != "" {
		t.Fatalf("RuntimeHandler = %q, want empty for CRI-O 1.24-shaped status", got.RuntimeHandler)
	}
	if got.PodSandboxID != "sandbox" || got.Labels["io.kubernetes.pod.uid"] != "pod-uid" || len(got.Mounts) != 1 {
		t.Fatalf("mapped status = %#v", got)
	}
}

func TestMapRuntimeEvent(t *testing.T) {
	event := mapRuntimeEvent(&runtimeapi.ContainerEventResponse{ContainerId: "id", ContainerEventType: runtimeapi.ContainerEventType_CONTAINER_STARTED_EVENT})
	if event.ContainerID != "id" || event.Type != runtimeEventStarted {
		t.Fatalf("event = %#v", event)
	}
}

func TestMapGRPCEventErrorMapsUnimplementedToUnsupported(t *testing.T) {
	err := mapGRPCEventError(status.Error(codes.Unimplemented, "events unsupported"))
	if !errors.Is(err, errEventsUnsupported) {
		t.Fatalf("mapGRPCEventError = %v, want %v", err, errEventsUnsupported)
	}
}
