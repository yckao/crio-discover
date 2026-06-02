package discover

import (
	"path/filepath"
	"strings"
)

type kubeletVolumeResolver struct {
	root string
}

func newKubeletVolumeResolver(root string) *kubeletVolumeResolver {
	if root == "" {
		return &kubeletVolumeResolver{root: ""}
	}
	return &kubeletVolumeResolver{root: filepath.Clean(root)}
}

func (r *kubeletVolumeResolver) Resolve(podUID string, mounts []runtimeMount) []Volume {
	volumes := make([]Volume, 0, len(mounts))
	for _, mount := range mounts {
		volumeType, name, source := r.infer(podUID, mount.HostPath)
		volumes = append(volumes, Volume{
			HostPath:      mount.HostPath,
			ContainerPath: mount.ContainerPath,
			ReadOnly:      mount.ReadOnly,
			Propagation:   mount.Propagation,
			Type:          volumeType,
			Name:          name,
			Source:        source,
		})
	}
	return volumes
}

func (r *kubeletVolumeResolver) infer(podUID, hostPath string) (VolumeType, string, string) {
	if podUID == "" || hostPath == "" || r.root == "" {
		return VolumeTypeUnknown, "", ""
	}
	cleanHostPath := filepath.Clean(hostPath)
	volumesPrefix := filepath.Join(r.root, "pods", podUID, "volumes") + string(filepath.Separator)
	if strings.HasPrefix(cleanHostPath, volumesPrefix) {
		rel, err := filepath.Rel(volumesPrefix, cleanHostPath)
		if err != nil {
			return VolumeTypeUnknown, "", ""
		}
		parts := strings.Split(rel, string(filepath.Separator))
		if len(parts) >= 2 {
			plugin := parts[0]
			name := parts[1]
			return volumeTypeForPlugin(plugin), name, plugin
		}
	}

	subpathsPrefix := filepath.Join(r.root, "pods", podUID, "volume-subpaths") + string(filepath.Separator)
	if strings.HasPrefix(cleanHostPath, subpathsPrefix) {
		rel, err := filepath.Rel(subpathsPrefix, cleanHostPath)
		if err != nil {
			return VolumeTypeUnknown, "", ""
		}
		parts := strings.Split(rel, string(filepath.Separator))
		if len(parts) >= 1 && parts[0] != "" {
			return VolumeTypeVolumeSubpaths, parts[0], "volume-subpaths"
		}
	}

	return VolumeTypeUnknown, "", ""
}

func volumeTypeForPlugin(plugin string) VolumeType {
	switch plugin {
	case "kubernetes.io~host-path":
		return VolumeTypeHostPath
	case "kubernetes.io~empty-dir":
		return VolumeTypeEmptyDir
	case "kubernetes.io~secret":
		return VolumeTypeSecret
	case "kubernetes.io~configmap":
		return VolumeTypeConfigMap
	case "kubernetes.io~projected":
		return VolumeTypeProjected
	case "kubernetes.io~downward-api":
		return VolumeTypeDownwardAPI
	case "kubernetes.io~csi":
		return VolumeTypePersistentVolumeClaim
	default:
		return VolumeTypeUnknown
	}
}
