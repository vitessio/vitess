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

// Package withddl allows you to automatically ensure
// the tables against which you want to apply statements
// are up-to-date.
package withddl

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/vt/sidecardb"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
)

var enableWithDDLForTests bool

const QueryToTriggerWithDDL = "SELECT _vt_no_such_column__init_schema FROM _vt.vreplication LIMIT 1"

// WithDDL allows you to execute statements against
// tables whose schema may not be up-to-date. If the tables
// don't exist or result in a schema error, it can apply a series
// of idempotent DDLs that will create or bring the tables
// to the desired state and retry.
type WithDDL struct {
	ddls []string
}

// New creates a new WithDDL.
func New(ddls []string) *WithDDL {
	return &WithDDL{
		ddls: ddls,
	}
}

// DDLs returns a copy of the ddls
func (wd *WithDDL) DDLs() []string {
	return wd.ddls[:]
}

// Exec executes the query using the supplied function.
// If there are any schema errors, it applies the DDLs and retries.
// It takes 2 functions, one to run the query and the other to run the
// DDL commands. This is generally needed so that different users can be used
// to run the commands. i.e. AllPrivs user for DDLs and App user for query commands.
// Funcs can be any of these types:
// func(query string) (*sqltypes.Result, error)
// func(query string, maxrows int) (*sqltypes.Result, error)
// func(query string, maxrows int, wantfields bool) (*sqltypes.Result, error)
// func(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error)
func (wd *WithDDL) Exec(ctx context.Context, query string, fQuery any, fDDL any) (*sqltypes.Result, error) {

	execQuery, err := wd.unify(ctx, fQuery)
	if err != nil {
		return nil, err
	}

	// if we are using the new schema init logic AND we are not running withddl unit tests
	if sidecardb.InitVTSchemaOnTabletInit && !enableWithDDLForTests {
		return execQuery(query)
	}

	execDDL, err := wd.unify(ctx, fDDL)
	if err != nil {
		return nil, err
	}
	qr, err := execQuery(query)
	if err == nil {
		return qr, nil
	}

	if !wd.isSchemaError(err) {
		return nil, err
	}

	log.Infof("Updating schema for %v and retrying: %v", sqlparser.TruncateForUI(err.Error()), err)
	for _, applyQuery := range wd.ddls {
		_, merr := execDDL(applyQuery)
		if merr == nil {
			continue
		}
		if mysql.IsSchemaApplyError(merr) {
			continue
		}
		log.Warningf("DDL apply %v failed: %v", applyQuery, merr)
		// Return the original error.
		return nil, err
	}
	return execQuery(query)
}

// ExecIgnore executes the query using the supplied function.
// If there are any schema errors, it returns an empty result.
func (wd *WithDDL) ExecIgnore(ctx context.Context, query string, f any) (*sqltypes.Result, error) {
	exec, err := wd.unify(ctx, f)
	if err != nil {
		return nil, err
	}
	qr, err := exec(query)
	if err == nil {
		return qr, nil
	}
	if !wd.isSchemaError(err) {
		return nil, err
	}
	return &sqltypes.Result{}, nil
}

func (wd *WithDDL) unify(ctx context.Context, f any) (func(query string) (*sqltypes.Result, error), error) {
	switch f := f.(type) {
	case func(query string) (*sqltypes.Result, error):
		return f, nil
	case func(query string, maxrows int) (*sqltypes.Result, error):
		return func(query string) (*sqltypes.Result, error) {
			return f(query, 10000)
		}, nil
	case func(query string, maxrows int, wantfields bool) (*sqltypes.Result, error):
		return func(query string) (*sqltypes.Result, error) {
			return f(query, 10000, true)
		}, nil
	case func(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error):
		return func(query string) (*sqltypes.Result, error) {
			return f(ctx, query, 10000, true)
		}, nil
	}
	return nil, fmt.Errorf("BUG: supplied function does not match expected signatures")
}

func (wd *WithDDL) isSchemaError(err error) bool {
	merr, isSQLErr := err.(*mysql.SQLError)
	if !isSQLErr {
		return false
	}
	switch merr.Num {
	case mysql.ERNoSuchTable, mysql.ERBadDb, mysql.ERWrongValueCountOnRow, mysql.ERBadFieldError:
		return true
	}
	return false
}
