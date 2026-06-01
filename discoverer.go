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

func (d *discoverer) Close() error {
	if d.client == nil {
		return nil
	}
	return d.client.Close()
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
