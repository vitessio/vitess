package inst

type MinimalInstance struct {
	Key         InstanceKey
	PrimaryKey  InstanceKey
	ClusterName string
}

func (minimalInstance *MinimalInstance) ToInstance() *Instance {
	return &Instance{
		Key:         minimalInstance.Key,
		SourceKey:   minimalInstance.PrimaryKey,
		ClusterName: minimalInstance.ClusterName,
	}
}
