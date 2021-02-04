/*
Copyright 2020 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildOtherReadAndAdmin(sql string, vschema ContextVSchema) (engine.Primitive, error) {
	destination, keyspace, _, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}

	if destination == nil {
		destination = key.DestinationAnyShard{}
	}

	return &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             sql, //This is original sql query to be passed as the parser can provide partial ddl AST.
		SingleShardOnly:   true,
	}, nil
}
