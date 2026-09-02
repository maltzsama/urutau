package v1alpha1

// DeepCopyInto copies the spec, deep-copying the nested slices/maps.
func (in *CDCPipelineSpec) DeepCopyInto(out *CDCPipelineSpec) {
	*out = *in
	if in.Definition.Tables != nil {
		out.Definition.Tables = make([]map[string]any, len(in.Definition.Tables))
		for i := range in.Definition.Tables {
			out.Definition.Tables[i] = copyMapAny(in.Definition.Tables[i])
		}
	}
	in.Coordinator.Resources.DeepCopyInto(&out.Coordinator.Resources)
	in.WorkerDefaults.Resources.DeepCopyInto(&out.WorkerDefaults.Resources)
}

// DeepCopy copies the spec.
func (in *CDCPipelineSpec) DeepCopy() *CDCPipelineSpec {
	if in == nil {
		return nil
	}
	out := new(CDCPipelineSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the status, deep-copying the pointer field.
func (in *CDCPipelineStatus) DeepCopyInto(out *CDCPipelineStatus) {
	*out = *in
	if in.Terminated != nil {
		t := *in.Terminated
		out.Terminated = &t
	}
}

// DeepCopy copies the status.
func (in *CDCPipelineStatus) DeepCopy() *CDCPipelineStatus {
	if in == nil {
		return nil
	}
	out := new(CDCPipelineStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies resource requirements.
func (in *ResourceRequirements) DeepCopyInto(out *ResourceRequirements) {
	*out = *in
	out.Requests = copyMap(in.Requests)
	out.Limits = copyMap(in.Limits)
}

// DeepCopy copies resource requirements.
func (in *ResourceRequirements) DeepCopy() *ResourceRequirements {
	if in == nil {
		return nil
	}
	out := new(ResourceRequirements)
	in.DeepCopyInto(out)
	return out
}

func copyMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func copyMapAny(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

var _ = copyMapAny
