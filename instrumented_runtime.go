package discover

import (
	"context"
	"time"
)

func (d *discoverer) listRuntimeContainers(ctx context.Context) ([]runtimeContainer, error) {
	started := time.Now()
	containers, err := d.client.ListContainers(ctx)
	d.observeRuntimeRPC("ListContainers", started, err)
	return containers, err
}

func (d *discoverer) runtimeContainerStatus(ctx context.Context, id string) (runtimeContainer, error) {
	started := time.Now()
	container, err := d.client.ContainerStatus(ctx, id)
	d.observeRuntimeRPC("ContainerStatus", started, err)
	return container, err
}

func (d *discoverer) runtimeWatchEvents(ctx context.Context) (runtimeEventStream, error) {
	started := time.Now()
	stream, err := d.client.WatchEvents(ctx)
	d.observeRuntimeRPC("GetContainerEvents", started, err)
	return stream, err
}
