/*
Copyright 2024 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtgate

import (
	"vitess.io/vitess/go/viperutil"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// DynamicViperConfig is a dynamic config that uses viper.
type DynamicViperConfig struct {
	onlineDDL viperutil.Value[bool]
	directDDL viperutil.Value[bool]
	txMode    viperutil.Value[vtgatepb.TransactionMode]
}

// NewDynamicViperConfig creates a new dynamic viper config
func NewDynamicViperConfig() *DynamicViperConfig {
	return &DynamicViperConfig{
		onlineDDL: enableOnlineDDL,
		directDDL: enableDirectDDL,
		txMode:    transactionMode,
	}
}

func (d *DynamicViperConfig) OnlineEnabled() bool {
	return d.onlineDDL.Get()
}

func (d *DynamicViperConfig) DirectEnabled() bool {
	return d.directDDL.Get()
}

func (d *DynamicViperConfig) TransactionMode() vtgatepb.TransactionMode {
	return d.txMode.Get()
}
