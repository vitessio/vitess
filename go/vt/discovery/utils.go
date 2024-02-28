/*
Copyright 2019 The Vitess Authors.

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

package discovery

import (
	"strings"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// This file contains helper filter methods to process the unfiltered list of
// tablets returned by HealthCheckImpl.GetTabletHealth*.

func ParseTabletTypesAndOrder(tabletTypesStr string) ([]topodatapb.TabletType, bool, error) {
	inOrder := false
	if strings.HasPrefix(tabletTypesStr, InOrderHint) {
		inOrder = true
		tabletTypesStr = tabletTypesStr[len(InOrderHint):]
	}
	tabletTypes, err := topoproto.ParseTabletTypes(tabletTypesStr)

	return tabletTypes, inOrder, err
}
