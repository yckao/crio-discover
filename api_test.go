package criodiscovery

import (
	"context"
	"testing"
	"time"
)

func TestPublicAPISurfaceCompiles(t *testing.T) {
	var _ Discoverer = (*fakeDiscovererForAPITest)(nil)

	cfg := DefaultConfig()
	cfg.CRISocketPath = "/var/run/crio/crio.sock"
	cfg.CachePath = "/tmp/crio-discovery-cache.json"
	cfg.NotificationTTL = time.Minute
	cfg.Predicates = []Predicate{func(c Container) bool { return c.ID != "" }}
	cfg.ErrorHandler = func(error) {}

	if cfg.CRISocketPath == "" {
		t.Fatal("default config must have a CRI socket path")
	}
}

type fakeDiscovererForAPITest struct{}

func (*fakeDiscovererForAPITest) List(context.Context) ([]Container, error) { return nil, nil }
func (*fakeDiscovererForAPITest) Watch(context.Context, Handler) error      { return nil }
func (*fakeDiscovererForAPITest) WatchChan(context.Context) (<-chan Container, <-chan error) {
	containers := make(chan Container)
	errs := make(chan error)
	close(containers)
	close(errs)
	return containers, errs
}
