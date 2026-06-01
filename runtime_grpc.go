package criodiscovery

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
