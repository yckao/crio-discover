package criodiscovery

import (
	"context"
	"errors"
)

var (
	errEventsUnsupported   = errors.New("CRI container events unsupported")
	errWatchNotImplemented = errors.New("watch is not implemented")
)

func (d *discoverer) Watch(context.Context, Handler) error {
	return errWatchNotImplemented
}

func (d *discoverer) WatchChan(context.Context) (<-chan Container, <-chan error) {
	containers := make(chan Container)
	errs := make(chan error, 1)
	errs <- errWatchNotImplemented
	close(containers)
	close(errs)
	return containers, errs
}
