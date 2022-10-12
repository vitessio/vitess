/*
Copyright 2022 The Vitess Authors.

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

package viperget

import (
	"github.com/spf13/viper"

	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

func TabletType(v *viper.Viper) func(key string) topodatapb.TabletType {
	if v == nil {
		v = viper.GetViper()
	}

	return func(key string) (ttype topodatapb.TabletType) {
		if !v.IsSet(key) {
			return
		}

		switch val := v.Get(key).(type) {
		case int:
			ttype = topodatapb.TabletType(int32(val))
		case string:
			var err error
			if ttype, err = topoproto.ParseTabletType(val); err != nil {
				// TODO: decide how we want to handle these cases.
				log.Warningf("GetTabletTypeValue failed to parse tablet type: %s. Defaulting to TabletType %s.\n", err.Error(), topoproto.TabletTypeLString(ttype))
			}
		default:
			// TODO: decide how we want to handle this case.
			log.Warningf("GetTabletTypeValue: invalid Go type %T for TabletType; must be string or int. Defaulting to TabletType %s.\n", val, ttype)
		}

		return
	}
}
