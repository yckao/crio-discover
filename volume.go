package criodiscovery

type kubeletVolumeResolver struct {
	root string
}

func newKubeletVolumeResolver(root string) *kubeletVolumeResolver {
	return &kubeletVolumeResolver{root: root}
}

func (r *kubeletVolumeResolver) Resolve(string, []runtimeMount) []Volume {
	return nil
}
