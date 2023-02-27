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

package planbuilder

import (
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func buildShowThrottledAppsPlan(query string, vschema plancontext.VSchema) (*planResult, error) {
	dest, ks, tabletType, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}
	if ks == nil {
		return nil, vterrors.VT09005()
	}

	if tabletType != topodatapb.TabletType_PRIMARY {
		return nil, vterrors.VT09007("SHOW")
	}

	if dest == nil {
		dest = key.DestinationAllShards{}
	}

	return newPlanResult(&engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             query,
	}), nil
}

func buildShowThrottlerStatusPlan(query string, vschema plancontext.VSchema) (*planResult, error) {
	dest, ks, tabletType, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}
	if ks == nil {
		return nil, vterrors.VT09005()
	}

	if tabletType != topodatapb.TabletType_PRIMARY {
		return nil, vterrors.VT09010()
	}

	if dest == nil {
		dest = key.DestinationAllShards{}
	}

	return newPlanResult(&engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             query,
	}), nil
}
