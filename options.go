package discover

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Option mutates Config before validation in New.
type Option func(*Config)

// DefaultConfig returns production-oriented defaults for CRI-O on Kubernetes nodes.
func DefaultConfig() Config {
	return Config{
		CRISocketPath:              "/var/run/crio/crio.sock",
		KubeletRoot:                "/var/lib/kubelet",
		PollInterval:               30 * time.Second,
		EnableEvents:               true,
		RuntimeRetryLimit:          3,
		RuntimeRetryInitialBackoff: 100 * time.Millisecond,
		RuntimeRetryMaxBackoff:     time.Second,
	}
}

// WithPredicate appends one predicate.
func WithPredicate(p Predicate) Option {
	return func(c *Config) {
		c.Predicates = append(c.Predicates, p)
	}
}

// WithPredicates appends multiple predicates.
func WithPredicates(predicates ...Predicate) Option {
	return func(c *Config) {
		c.Predicates = append(c.Predicates, predicates...)
	}
}

// WithPollInterval sets the polling interval used by Watch and WatchChan.
func WithPollInterval(d time.Duration) Option {
	return func(c *Config) {
		c.PollInterval = d
	}
}

// WithEvents enables or disables CRI event acceleration.
func WithEvents(enabled bool) Option {
	return func(c *Config) {
		c.EnableEvents = enabled
	}
}

// WithRuntimeRetry configures bounded exponential retry for CRI runtime RPC timeouts.
// limit is the number of retries after the initial attempt. Set limit to 0 to disable retries.
func WithRuntimeRetry(limit int, initialBackoff, maxBackoff time.Duration) Option {
	return func(c *Config) {
		c.RuntimeRetryLimit = limit
		c.RuntimeRetryInitialBackoff = initialBackoff
		c.RuntimeRetryMaxBackoff = maxBackoff
	}
}

// WithCache configures persistent notification dedupe.
func WithCache(path string, ttl time.Duration) Option {
	return func(c *Config) {
		c.CachePath = path
		c.NotificationTTL = ttl
	}
}

// WithErrorHandler configures callback-watch reporting for recoverable errors.
func WithErrorHandler(h ErrorHandler) Option {
	return func(c *Config) {
		c.ErrorHandler = h
	}
}

// WithLogger configures structured diagnostic logging. A nil logger disables logging.
func WithLogger(logger *slog.Logger) Option {
	return func(c *Config) {
		c.Logger = logger
	}
}

// WithPrometheus registers low-cardinality discovery metrics with registerer.
// A nil registerer disables Prometheus metrics.
func WithPrometheus(registerer prometheus.Registerer) Option {
	return func(c *Config) {
		c.PrometheusRegisterer = registerer
	}
}

func applyOptions(config *Config, opts ...Option) error {
	for i, opt := range opts {
		if opt == nil {
			return fmt.Errorf("option %d is nil", i)
		}
		opt(config)
	}
	return validateConfig(*config)
}

func validateConfig(config Config) error {
	if config.CRISocketPath == "" {
		return fmt.Errorf("CRI socket path is required")
	}
	if config.KubeletRoot == "" {
		return fmt.Errorf("kubelet root is required")
	}
	if config.PollInterval <= 0 {
		return fmt.Errorf("poll interval must be positive")
	}
	if config.RuntimeRetryLimit < 0 {
		return fmt.Errorf("runtime retry limit must not be negative")
	}
	if config.RuntimeRetryInitialBackoff < 0 {
		return fmt.Errorf("runtime retry initial backoff must not be negative")
	}
	if config.RuntimeRetryMaxBackoff < 0 {
		return fmt.Errorf("runtime retry max backoff must not be negative")
	}
	if config.RuntimeRetryLimit > 0 && config.RuntimeRetryMaxBackoff < config.RuntimeRetryInitialBackoff {
		return fmt.Errorf("runtime retry max backoff must be greater than or equal to initial backoff")
	}
	if config.NotificationTTL < 0 {
		return fmt.Errorf("notification TTL must not be negative")
	}
	if config.NotificationTTL > 0 && config.CachePath == "" {
		return fmt.Errorf("cache path is required when notification TTL is positive")
	}
	for i, predicate := range config.Predicates {
		if predicate == nil {
			return fmt.Errorf("predicate %d is nil", i)
		}
	}
	return nil
}
