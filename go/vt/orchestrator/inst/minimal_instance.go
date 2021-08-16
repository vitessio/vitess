package inst

type MinimalInstance struct {
	Key         InstanceKey
	PrimaryKey  InstanceKey
	ClusterName string
}

func (this *MinimalInstance) ToInstance() *Instance {
	return &Instance{
		Key:         this.Key,
		PrimaryKey:  this.PrimaryKey,
		ClusterName: this.ClusterName,
	}
}
