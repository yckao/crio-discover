package criodiscovery

import (
	"context"
	"errors"
)

var errEventsUnsupported = errors.New("CRI container events unsupported")

func (d *discoverer) Watch(context.Context, Handler) error {
	return errors.New("watch is not implemented")
}

func (d *discoverer) WatchChan(context.Context) (<-chan Container, <-chan error) {
	containers := make(chan Container)
	errs := make(chan error)
	close(containers)
	close(errs)
	return containers, errs
}
