/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package splitquery

import (
	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// SQLExecuter enacpsulates access to the MySQL database for the this package.
type SQLExecuter interface {
	SQLExecute(sql string, bindVariables map[string]*querypb.BindVariable) (*sqltypes.Result, error)
}

// Command to generate a mock for this interface with mockgen.
//go:generate mockgen -source $GOFILE -destination splitquery_testing/mock_sqlexecuter.go  -package splitquery_testing
