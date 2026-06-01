package criodiscover

import (
	"strings"
	"testing"
	"time"
)

func TestOptionsApplyToConfig(t *testing.T) {
	cfg := DefaultConfig()
	err := applyOptions(&cfg,
		WithPollInterval(5*time.Second),
		WithEvents(false),
		WithCache("/tmp/cache.json", time.Hour),
		WithPredicate(func(Container) bool { return true }),
		WithPredicates(func(Container) bool { return true }),
		WithErrorHandler(func(error) {}),
	)
	if err != nil {
		t.Fatalf("apply options: %v", err)
	}
	if cfg.PollInterval != 5*time.Second {
		t.Fatalf("poll interval = %s", cfg.PollInterval)
	}
	if cfg.EnableEvents {
		t.Fatal("events should be disabled")
	}
	if cfg.CachePath != "/tmp/cache.json" || cfg.NotificationTTL != time.Hour {
		t.Fatalf("cache config = %q %s", cfg.CachePath, cfg.NotificationTTL)
	}
	if len(cfg.Predicates) != 2 {
		t.Fatalf("predicates = %d", len(cfg.Predicates))
	}
	if cfg.ErrorHandler == nil {
		t.Fatal("error handler should be set")
	}
}

func TestValidateConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
		want string
	}{
		{name: "empty socket", edit: func(c *Config) { c.CRISocketPath = "" }, want: "CRI socket path"},
		{name: "empty kubelet root", edit: func(c *Config) { c.KubeletRoot = "" }, want: "kubelet root"},
		{name: "bad poll interval", edit: func(c *Config) { c.PollInterval = 0 }, want: "poll interval"},
		{name: "negative ttl", edit: func(c *Config) { c.NotificationTTL = -time.Second }, want: "notification TTL"},
		{name: "ttl without path", edit: func(c *Config) { c.NotificationTTL = time.Minute; c.CachePath = "" }, want: "cache path"},
		{name: "nil predicate", edit: func(c *Config) { c.Predicates = []Predicate{nil} }, want: "predicate 0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.edit(&cfg)
			err := validateConfig(cfg)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %q does not contain %q", err.Error(), tt.want)
			}
		})
	}
}

func TestApplyOptionsRejectsNilOption(t *testing.T) {
	cfg := DefaultConfig()
	err := applyOptions(&cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "option 0") {
		t.Fatalf("expected option error, got %v", err)
	}
}
