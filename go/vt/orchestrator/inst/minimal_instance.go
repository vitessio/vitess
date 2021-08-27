package inst

type MinimalInstance struct {
	Key         InstanceKey
	PrimaryKey  InstanceKey
	ClusterName string
}

func (this *MinimalInstance) ToInstance() *Instance {
	return &Instance{
		Key:         this.Key,
		SourceKey:   this.PrimaryKey,
		ClusterName: this.ClusterName,
	}
}
