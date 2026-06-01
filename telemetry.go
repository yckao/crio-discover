package discover

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/status"
)

type telemetry struct {
	logger  *slog.Logger
	metrics *prometheusMetrics
}

type prometheusMetrics struct {
	runtimeRPCTotal    *prometheus.CounterVec
	runtimeRPCDuration *prometheus.HistogramVec
	recoverableErrors  *prometheus.CounterVec
	scans              *prometheus.CounterVec
	notifications      *prometheus.CounterVec
	cacheEvents        *prometheus.CounterVec
	eventStreamEvents  *prometheus.CounterVec
}

func newTelemetry(config Config) (*telemetry, error) {
	telemetry := &telemetry{logger: config.Logger}
	if config.PrometheusRegisterer == nil {
		if telemetry.logger == nil {
			return nil, nil
		}
		return telemetry, nil
	}
	metrics, err := newPrometheusMetrics(config.PrometheusRegisterer)
	if err != nil {
		return nil, err
	}
	telemetry.metrics = metrics
	return telemetry, nil
}

func newPrometheusMetrics(registerer prometheus.Registerer) (*prometheusMetrics, error) {
	runtimeRPCTotal, err := registerCounterVec(registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "crio_discover",
		Name:      "runtime_rpc_total",
		Help:      "Total CRI runtime RPC calls by method and gRPC status code.",
	}, []string{"method", "code"}))
	if err != nil {
		return nil, err
	}
	runtimeRPCDuration, err := registerHistogramVec(registerer, prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "crio_discover",
		Name:      "runtime_rpc_duration_seconds",
		Help:      "CRI runtime RPC latency by method and gRPC status code.",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "code"}))
	if err != nil {
		return nil, err
	}
	recoverableErrors, err := registerCounterVec(registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "crio_discover",
		Name:      "recoverable_errors_total",
		Help:      "Total recoverable discovery errors by operation.",
	}, []string{"operation"}))
	if err != nil {
		return nil, err
	}
	scans, err := registerCounterVec(registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "crio_discover",
		Name:      "scans_total",
		Help:      "Total discovery scans by source and result.",
	}, []string{"source", "result"}))
	if err != nil {
		return nil, err
	}
	notifications, err := registerCounterVec(registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "crio_discover",
		Name:      "notifications_total",
		Help:      "Total container discovery notifications by source.",
	}, []string{"source"}))
	if err != nil {
		return nil, err
	}
	cacheEvents, err := registerCounterVec(registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "crio_discover",
		Name:      "cache_events_total",
		Help:      "Total dedupe cache events by event type.",
	}, []string{"event"}))
	if err != nil {
		return nil, err
	}
	eventStreamEvents, err := registerCounterVec(registerer, prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "crio_discover",
		Name:      "event_stream_events_total",
		Help:      "Total CRI event stream lifecycle events by event type.",
	}, []string{"event"}))
	if err != nil {
		return nil, err
	}
	return &prometheusMetrics{
		runtimeRPCTotal:    runtimeRPCTotal,
		runtimeRPCDuration: runtimeRPCDuration,
		recoverableErrors:  recoverableErrors,
		scans:              scans,
		notifications:      notifications,
		cacheEvents:        cacheEvents,
		eventStreamEvents:  eventStreamEvents,
	}, nil
}

func registerCounterVec(registerer prometheus.Registerer, collector *prometheus.CounterVec) (*prometheus.CounterVec, error) {
	if err := registerer.Register(collector); err != nil {
		var already prometheus.AlreadyRegisteredError
		if errors.As(err, &already) {
			existing, ok := already.ExistingCollector.(*prometheus.CounterVec)
			if !ok {
				return nil, fmt.Errorf("register prometheus counter: %w", err)
			}
			return existing, nil
		}
		return nil, fmt.Errorf("register prometheus counter: %w", err)
	}
	return collector, nil
}

func registerHistogramVec(registerer prometheus.Registerer, collector *prometheus.HistogramVec) (*prometheus.HistogramVec, error) {
	if err := registerer.Register(collector); err != nil {
		var already prometheus.AlreadyRegisteredError
		if errors.As(err, &already) {
			existing, ok := already.ExistingCollector.(*prometheus.HistogramVec)
			if !ok {
				return nil, fmt.Errorf("register prometheus histogram: %w", err)
			}
			return existing, nil
		}
		return nil, fmt.Errorf("register prometheus histogram: %w", err)
	}
	return collector, nil
}

func (t *telemetry) observeRuntimeRPC(method string, duration time.Duration, err error) {
	if t == nil || t.metrics == nil {
		return
	}
	code := prometheusErrorCode(err)
	t.metrics.runtimeRPCTotal.WithLabelValues(method, code).Inc()
	t.metrics.runtimeRPCDuration.WithLabelValues(method, code).Observe(duration.Seconds())
}

func (t *telemetry) observeRecoverableError(ctx context.Context, operation, containerID string, err error) {
	if t == nil || err == nil {
		return
	}
	if t.metrics != nil {
		t.metrics.recoverableErrors.WithLabelValues(operation).Inc()
	}
	if t.logger != nil {
		attrs := []any{"operation", operation, "error", err}
		if containerID != "" {
			attrs = append(attrs, "container_id", containerID)
		}
		t.logger.WarnContext(ctx, "recoverable discovery error", attrs...)
	}
}

func (t *telemetry) observeScan(source, result string) {
	if t == nil || t.metrics == nil {
		return
	}
	t.metrics.scans.WithLabelValues(source, result).Inc()
}

func (t *telemetry) observeNotification(source string) {
	if t == nil || t.metrics == nil {
		return
	}
	t.metrics.notifications.WithLabelValues(source).Inc()
}

func (t *telemetry) observeCacheEvent(event string) {
	if t == nil || t.metrics == nil {
		return
	}
	t.metrics.cacheEvents.WithLabelValues(event).Inc()
}

func (t *telemetry) observeEventStreamEvent(ctx context.Context, event string, err error) {
	if t == nil {
		return
	}
	if t.metrics != nil {
		t.metrics.eventStreamEvents.WithLabelValues(event).Inc()
	}
	if t.logger != nil {
		if err != nil {
			t.logger.DebugContext(ctx, "CRI event stream state", "event", event, "error", err)
			return
		}
		t.logger.DebugContext(ctx, "CRI event stream state", "event", event)
	}
}

func prometheusErrorCode(err error) string {
	if err == nil {
		return "OK"
	}
	return status.Code(err).String()
}

func (d *discoverer) observeRuntimeRPC(method string, started time.Time, err error) {
	if d.telemetry != nil {
		d.telemetry.observeRuntimeRPC(method, time.Since(started), err)
	}
}

func (d *discoverer) reportRecoverableTo(ctx context.Context, report func(error), operation, containerID string, err error) {
	if err == nil {
		return
	}
	if d.telemetry != nil {
		d.telemetry.observeRecoverableError(ctx, operation, containerID, err)
	} else if d.config.Logger != nil {
		attrs := []any{"operation", operation, "error", err}
		if containerID != "" {
			attrs = append(attrs, "container_id", containerID)
		}
		d.config.Logger.WarnContext(ctx, "recoverable discovery error", attrs...)
	}
	if report != nil {
		report(err)
	}
}

func (d *discoverer) observeScan(source, result string) {
	if d.telemetry != nil {
		d.telemetry.observeScan(source, result)
	}
}

func (d *discoverer) observeNotification(source string) {
	if d.telemetry != nil {
		d.telemetry.observeNotification(source)
	}
}

func (d *discoverer) observeCacheEvent(event string) {
	if d.telemetry != nil {
		d.telemetry.observeCacheEvent(event)
	}
}

func (d *discoverer) observeEventStreamEvent(ctx context.Context, event string, err error) {
	if d.telemetry != nil {
		d.telemetry.observeEventStreamEvent(ctx, event, err)
	} else if d.config.Logger != nil {
		if err != nil {
			d.config.Logger.DebugContext(ctx, "CRI event stream state", "event", event, "error", err)
			return
		}
		d.config.Logger.DebugContext(ctx, "CRI event stream state", "event", event)
	}
}
