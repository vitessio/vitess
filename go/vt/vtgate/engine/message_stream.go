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

package engine

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*MessageStream)(nil)

// MessageStream is an operator for message streaming from specific keyspace, destination
type MessageStream struct {
	// Keyspace specifies the keyspace to stream messages from
	Keyspace *vindexes.Keyspace

	// TargetDestination specifies an explicit target destination to stream messages from
	TargetDestination key.Destination

	// TableName specifies the table on which stream will be executed.
	TableName string

	noTxNeeded

	noInputs
}

// RouteType implements the Primitive interface
func (s *MessageStream) RouteType() string {
	return "MessageStream"
}

// GetKeyspaceName implements the Primitive interface
func (s *MessageStream) GetKeyspaceName() string {
	return s.Keyspace.Name
}

// GetTableName implements the Primitive interface
func (s *MessageStream) GetTableName() string {
	return s.TableName
}

// Execute implements the Primitive interface
func (s *MessageStream) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	panic("implement me")
}

// StreamExecute implements the Primitive interface
func (s *MessageStream) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	rss, _, err := vcursor.ResolveDestinations(s.Keyspace.Name, nil, []key.Destination{s.TargetDestination})
	if err != nil {
		return err
	}
	return vcursor.MessageStream(rss, s.TableName, callback)
}

// GetFields implements the Primitive interface
func (s *MessageStream) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	panic("implement me")
}

func (s *MessageStream) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType:      "MessageStream",
		Keyspace:          s.Keyspace,
		TargetDestination: s.TargetDestination,

		Other: map[string]interface{}{"Table": s.TableName},
	}
}
