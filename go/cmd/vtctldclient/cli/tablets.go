/*
Copyright 2021 The Vitess Authors.

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

package cli

import (
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TabletAliasesFromPosArgs takes a list of positional (non-flag) arguments and
// converts them to tablet aliases.
func TabletAliasesFromPosArgs(args []string) ([]*topodatapb.TabletAlias, error) {
	aliases := make([]*topodatapb.TabletAlias, 0, len(args))

	for _, arg := range args {
		alias, err := topoproto.ParseTabletAlias(arg)
		if err != nil {
			return nil, err
		}

		aliases = append(aliases, alias)
	}

	return aliases, nil
}
