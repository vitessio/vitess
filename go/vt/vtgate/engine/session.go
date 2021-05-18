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
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type Session struct {
	Action func(sa SessionActions) (*sqltypes.Result, error)

	noInputs
	noTxNeeded
}

var _ Primitive = (*Session)(nil)

func (s Session) RouteType() string {
	return "SHOW"
}

func (s Session) GetKeyspaceName() string {
	return ""
}

func (s Session) GetTableName() string {
	return ""
}

func (s Session) Execute(vcursor VCursor, _ map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	return s.Action(vcursor.Session())
}

func (s Session) StreamExecute(vcursor VCursor, _ map[string]*querypb.BindVariable, _ bool, callback func(*sqltypes.Result) error) error {
	qr, err := s.Action(vcursor.Session())
	if err != nil {
		return err
	}
	return callback(qr)
}

func (s Session) GetFields(_ VCursor, _ map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	panic("implement me")
}

func (s Session) description() PrimitiveDescription {
	panic("implement me")
}
