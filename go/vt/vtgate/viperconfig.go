package vtgate

import "vitess.io/vitess/go/viperutil"

type dynamicViperConfig struct {
	onlineDDL viperutil.Value[bool]
	directDDL viperutil.Value[bool]
}

func (d *dynamicViperConfig) OnlineEnabled() bool {
	return d.onlineDDL.Get()
}

func (d *dynamicViperConfig) DirectEnabled() bool {
	return d.directDDL.Get()
}
