package criodiscover_test

import (
	"testing"

	"github.com/yckao/crio-discover"
)

func TestProjectRenameExposesNewModuleAndPackageName(t *testing.T) {
	cfg := criodiscover.DefaultConfig()
	if cfg.CRISocketPath == "" {
		t.Fatal("default config must have a CRI socket path")
	}
}
