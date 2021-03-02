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

package planbuilder

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// DropCreateDB is the interface that you need to implement to add a custom CREATE/DROP DATABASE handler
type DropCreateDB interface {
	CreateDatabase(ast *sqlparser.CreateDatabase) error
	DropDatabase(ast *sqlparser.DropDatabase) error
}

type defaultHandler struct{}

// CreateDatabase implements the DropCreateDB interface
func (defaultHandler) CreateDatabase(*sqlparser.CreateDatabase) error {
	return vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "create database is not supported")
}

// DropDatabase implements the DropCreateDB interface
func (defaultHandler) DropDatabase(*sqlparser.DropDatabase) error {
	return vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "drop database is not supported")
}

//goland:noinspection GoVarAndConstTypeMayBeOmitted
var databaseCreator DropCreateDB = defaultHandler{}
