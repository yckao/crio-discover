package discover

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestWithLoggerLogsRecoverableWatchErrors(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	client := &fakeRuntimeClient{listErr: errorsForTelemetryTest("temporary list failure"), listErrAfter: 1}
	cfg := DefaultConfig()
	if err := applyOptions(&cfg, WithLogger(logger), WithEvents(false), WithPollInterval(time.Millisecond)); err != nil {
		t.Fatalf("apply options: %v", err)
	}
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := d.Watch(ctx, func(context.Context, Container) error { return nil }); !errorsIsDeadline(err) {
		t.Fatalf("Watch error = %v, want deadline", err)
	}

	got := logs.String()
	if !strings.Contains(got, "recoverable discovery error") || !strings.Contains(got, "list_containers") || !strings.Contains(got, "temporary list failure") {
		t.Fatalf("logs = %q, want recoverable list error", got)
	}
}

func TestWithPrometheusRecordsRuntimeRPCMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	cfg := DefaultConfig()
	if err := applyOptions(&cfg, WithPrometheus(registry)); err != nil {
		t.Fatalf("apply options: %v", err)
	}
	telemetry, err := newTelemetry(cfg)
	if err != nil {
		t.Fatalf("new telemetry: %v", err)
	}
	client := &fakeRuntimeClient{
		listed: []runtimeContainer{{ID: "running", State: ContainerStateRunning}},
		statuses: map[string]runtimeContainer{
			"running": {ID: "running", Name: "app", State: ContainerStateRunning},
		},
	}
	d := &discoverer{config: cfg, client: client, resolver: staticVolumeResolver{}, telemetry: telemetry}

	if _, err := d.List(context.Background()); err != nil {
		t.Fatalf("List: %v", err)
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	if got := counterValue(families, "crio_discover_runtime_rpc_total", map[string]string{"method": "ListContainers", "code": "OK"}); got != 1 {
		t.Fatalf("ListContainers counter = %v, want 1", got)
	}
	if got := counterValue(families, "crio_discover_runtime_rpc_total", map[string]string{"method": "ContainerStatus", "code": "OK"}); got != 1 {
		t.Fatalf("ContainerStatus counter = %v, want 1", got)
	}
}

type errorsForTelemetryTest string

func (e errorsForTelemetryTest) Error() string { return string(e) }

func errorsIsDeadline(err error) bool {
	return err != nil && strings.Contains(err.Error(), context.DeadlineExceeded.Error())
}

func counterValue(families []*dto.MetricFamily, name string, labels map[string]string) float64 {
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if metricLabelsMatch(metric, labels) && metric.GetCounter() != nil {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func metricLabelsMatch(metric *dto.Metric, want map[string]string) bool {
	got := map[string]string{}
	for _, label := range metric.GetLabel() {
		got[label.GetName()] = label.GetValue()
	}
	for key, value := range want {
		if got[key] != value {
			return false
		}
	}
	return true
}
