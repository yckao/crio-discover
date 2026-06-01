package criodiscovery

import "time"

// Option mutates Config before validation in New.
type Option func(*Config)

// DefaultConfig returns production-oriented defaults for CRI-O on Kubernetes nodes.
func DefaultConfig() Config {
	return Config{
		CRISocketPath: "/var/run/crio/crio.sock",
		KubeletRoot:   "/var/lib/kubelet",
		PollInterval:  30 * time.Second,
		EnableEvents:  true,
	}
}
