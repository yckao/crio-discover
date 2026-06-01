package criodiscovery

import (
	"context"
	"errors"
)

var errRuntimeClientNotImplemented = errors.New("grpc runtime client is not implemented")

type grpcRuntimeClient struct{}

func newGRPCRuntimeClient(string) (runtimeClient, error) {
	return &grpcRuntimeClient{}, nil
}

func (*grpcRuntimeClient) ListContainers(context.Context) ([]runtimeContainer, error) {
	return nil, errRuntimeClientNotImplemented
}

func (*grpcRuntimeClient) ContainerStatus(context.Context, string) (runtimeContainer, error) {
	return runtimeContainer{}, errRuntimeClientNotImplemented
}

func (*grpcRuntimeClient) WatchEvents(context.Context) (runtimeEventStream, error) {
	return nil, errEventsUnsupported
}

func (*grpcRuntimeClient) Close() error { return nil }
