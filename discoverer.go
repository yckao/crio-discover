package discover

import (
	"context"
	"fmt"
)

type volumeResolver interface {
	Resolve(podUID string, mounts []runtimeMount) []Volume
}

type discoverer struct {
	config    Config
	client    runtimeClient
	resolver  volumeResolver
	telemetry *telemetry
}

// New validates config, applies options, and returns the default Discoverer implementation.
func New(config Config, opts ...Option) (Discoverer, error) {
	if err := applyOptions(&config, opts...); err != nil {
		return nil, err
	}
	telemetry, err := newTelemetry(config)
	if err != nil {
		return nil, err
	}
	client, err := newGRPCRuntimeClient(config.CRISocketPath)
	if err != nil {
		return nil, err
	}
	return &discoverer{
		config:    config,
		client:    client,
		resolver:  newKubeletVolumeResolver(config.KubeletRoot),
		telemetry: telemetry,
	}, nil
}

func (d *discoverer) Close() error {
	if d.client == nil {
		return nil
	}
	return d.client.Close()
}

func (d *discoverer) List(ctx context.Context) ([]Container, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if d.client == nil {
		return nil, ErrMissingRuntimeClient
	}
	containers, err := d.listRuntimeContainers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list containers: %w", err)
	}
	result := make([]Container, 0, len(containers))
	for _, candidate := range containers {
		if candidate.State != ContainerStateRunning {
			continue
		}
		status, err := d.runtimeContainerStatus(ctx, candidate.ID)
		if err != nil {
			if isContainerGoneError(err) {
				continue
			}
			return nil, fmt.Errorf("container status %s: %w", candidate.ID, err)
		}
		if status.State != ContainerStateRunning {
			continue
		}
		status = runtimeContainerWithFallback(status, candidate)
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
	podID := rc.PodSandboxID
	if podID == "" {
		podID = kubernetes.SandboxID
	}
	resolver := d.resolver
	if resolver == nil {
		resolver = newKubeletVolumeResolver(d.config.KubeletRoot)
	}
	return Container{
		ID:          rc.ID,
		PodID:       podID,
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

func runtimeContainerWithFallback(status, fallback runtimeContainer) runtimeContainer {
	if status.PodSandboxID == "" {
		status.PodSandboxID = fallback.PodSandboxID
	}
	if status.Name == "" {
		status.Name = fallback.Name
	}
	if status.Image == "" {
		status.Image = fallback.Image
	}
	if status.ImageRef == "" {
		status.ImageRef = fallback.ImageRef
	}
	if status.ImageID == "" {
		status.ImageID = fallback.ImageID
	}
	if status.RuntimeHandler == "" {
		status.RuntimeHandler = fallback.RuntimeHandler
	}
	if status.CreatedAt.IsZero() {
		status.CreatedAt = fallback.CreatedAt
	}
	if len(status.Labels) == 0 {
		status.Labels = fallback.Labels
	}
	if len(status.Annotations) == 0 {
		status.Annotations = fallback.Annotations
	}
	return status
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
