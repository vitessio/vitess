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
	"context"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// SessionPrimitive the session primitive is a very small primitive used
// when we have simple engine code that needs to interact with the Session
type SessionPrimitive struct {
	action func(sa SessionActions) (*sqltypes.Result, error)
	name   string

	noInputs
	noTxNeeded
}

var _ Primitive = (*SessionPrimitive)(nil)

// NewSessionPrimitive creates a SessionPrimitive
func NewSessionPrimitive(name string, action func(sa SessionActions) (*sqltypes.Result, error)) *SessionPrimitive {
	return &SessionPrimitive{
		action: action,
		name:   name,
	}
}

// RouteType implements the Primitive interface
func (s *SessionPrimitive) RouteType() string {
	return "SHOW"
}

// GetKeyspaceName implements the Primitive interface
func (s *SessionPrimitive) GetKeyspaceName() string {
	return ""
}

// GetTableName implements the Primitive interface
func (s *SessionPrimitive) GetTableName() string {
	return ""
}

// TryExecute implements the Primitive interface
func (s *SessionPrimitive) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	return s.action(vcursor.Session())
}

// TryStreamExecute implements the Primitive interface
func (s *SessionPrimitive) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	qr, err := s.action(vcursor.Session())
	if err != nil {
		return err
	}
	return callback(qr)
}

// GetFields implements the Primitive interface
func (s *SessionPrimitive) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "not supported for this primitive")
}

// description implements the Primitive interface
func (s *SessionPrimitive) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: s.name,
	}
}
