package criodiscover

import (
	"context"
	"errors"
	"io"
	"sync"
)

type fakeRuntimeClient struct {
	mu           sync.Mutex
	listed       []runtimeContainer
	statuses     map[string]runtimeContainer
	listErr      error
	listErrAfter int
	listCalls    int
	statusErr    map[string]error
	eventStream  runtimeEventStream
	eventErr     error
	closed       bool
	statusCalls  []string
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
	f.statusCalls = append(f.statusCalls, id)
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
