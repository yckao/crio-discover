package discover

import (
	"path/filepath"
	"testing"
)

func TestVolumeResolverInfersKnownKubeletVolumeTypes(t *testing.T) {
	root := t.TempDir()
	podUID := "pod-uid"
	tests := []struct {
		plugin string
		name   string
		want   VolumeType
	}{
		{plugin: "kubernetes.io~host-path", name: "host", want: VolumeTypeHostPath},
		{plugin: "kubernetes.io~empty-dir", name: "cache", want: VolumeTypeEmptyDir},
		{plugin: "kubernetes.io~secret", name: "secret", want: VolumeTypeSecret},
		{plugin: "kubernetes.io~configmap", name: "config", want: VolumeTypeConfigMap},
		{plugin: "kubernetes.io~projected", name: "projected", want: VolumeTypeProjected},
		{plugin: "kubernetes.io~downward-api", name: "downward", want: VolumeTypeDownwardAPI},
		{plugin: "kubernetes.io~csi", name: "pvc", want: VolumeTypePersistentVolumeClaim},
	}

	resolver := newKubeletVolumeResolver(root)
	for _, tt := range tests {
		t.Run(tt.plugin, func(t *testing.T) {
			hostPath := filepath.Join(root, "pods", podUID, "volumes", tt.plugin, tt.name)
			volumes := resolver.Resolve(podUID, []runtimeMount{{HostPath: hostPath, ContainerPath: "/mnt", ReadOnly: true, Propagation: "PROPAGATION_PRIVATE"}})
			if len(volumes) != 1 {
				t.Fatalf("volumes = %d", len(volumes))
			}
			got := volumes[0]
			if got.Type != tt.want || got.Name != tt.name || got.Source != tt.plugin {
				t.Fatalf("volume = %#v", got)
			}
			if got.HostPath != hostPath || got.ContainerPath != "/mnt" || !got.ReadOnly || got.Propagation != "PROPAGATION_PRIVATE" {
				t.Fatalf("mount fields = %#v", got)
			}
		})
	}
}

func TestVolumeResolverHandlesVolumeSubpaths(t *testing.T) {
	root := t.TempDir()
	podUID := "pod-uid"
	resolver := newKubeletVolumeResolver(root)
	hostPath := filepath.Join(root, "pods", podUID, "volume-subpaths", "config", "app", "0")
	volumes := resolver.Resolve(podUID, []runtimeMount{{HostPath: hostPath, ContainerPath: "/etc/config"}})
	if len(volumes) != 1 {
		t.Fatalf("volumes = %d", len(volumes))
	}
	if volumes[0].Name != "config" || volumes[0].Type != VolumeTypeUnknown || volumes[0].Source != "volume-subpaths" {
		t.Fatalf("volume = %#v", volumes[0])
	}
}

func TestVolumeResolverEmptyRootDoesNotClassifyRelativePaths(t *testing.T) {
	podUID := "pod-uid"
	resolver := newKubeletVolumeResolver("")
	hostPath := filepath.Join("pods", podUID, "volumes", "kubernetes.io~secret", "name")
	volumes := resolver.Resolve(podUID, []runtimeMount{{HostPath: hostPath, ContainerPath: "/etc/secret"}})
	if len(volumes) != 1 {
		t.Fatalf("volumes = %d", len(volumes))
	}
	if volumes[0].Type != VolumeTypeUnknown || volumes[0].Name != "" || volumes[0].Source != "" {
		t.Fatalf("volume = %#v", volumes[0])
	}
}

func TestVolumeResolverPreservesUnsupportedPluginNameAndSource(t *testing.T) {
	root := t.TempDir()
	podUID := "pod-uid"
	resolver := newKubeletVolumeResolver(root)
	hostPath := filepath.Join(root, "pods", podUID, "volumes", "kubernetes.io~nfs", "share")
	volumes := resolver.Resolve(podUID, []runtimeMount{{HostPath: hostPath, ContainerPath: "/data"}})
	if len(volumes) != 1 {
		t.Fatalf("volumes = %d", len(volumes))
	}
	if volumes[0].Type != VolumeTypeUnknown || volumes[0].Name != "share" || volumes[0].Source != "kubernetes.io~nfs" {
		t.Fatalf("volume = %#v", volumes[0])
	}
}

func TestVolumeResolverFallsBackToUnknown(t *testing.T) {
	resolver := newKubeletVolumeResolver("/var/lib/kubelet")
	volumes := resolver.Resolve("", []runtimeMount{{HostPath: "/opt/data", ContainerPath: "/data"}})
	if len(volumes) != 1 {
		t.Fatalf("volumes = %d", len(volumes))
	}
	if volumes[0].Type != VolumeTypeUnknown || volumes[0].Name != "" || volumes[0].Source != "" {
		t.Fatalf("volume = %#v", volumes[0])
	}
}
