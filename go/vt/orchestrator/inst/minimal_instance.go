package inst

type MinimalInstance struct {
	Key         InstanceKey
	MasterKey   InstanceKey
	ClusterName string
}

func (this *MinimalInstance) ToInstance() *Instance {
	return &Instance{
		Key:         this.Key,
		PrimaryKey:  this.MasterKey,
		ClusterName: this.ClusterName,
	}
}
