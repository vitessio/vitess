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

package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildPlanForBypass(stmt sqlparser.Statement, vschema ContextVSchema) (engine.Primitive, error) {
	keyspace, err := vschema.DefaultKeyspace()
	if err != nil {
		return nil, err
	}
	return &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: vschema.Destination(),
		TargetTabletType:  vschema.TabletType(),
		Query:             sqlparser.String(stmt),
	}, nil
}
