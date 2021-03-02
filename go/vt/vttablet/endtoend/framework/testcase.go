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

package framework

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Testable restricts the types that can be added to
// a test case.
type Testable interface {
	Test(name string, client *QueryClient) error
	Benchmark(client *QueryClient) error
}

var (
	_ Testable = TestQuery("")
	_ Testable = &TestCase{}
	_ Testable = &MultiCase{}
)

// TestQuery represents a plain query. It will be
// executed without a bind variable. The framework
// will check for errors, but nothing beyond. Statements
// like 'begin', etc. will be converted to the corresponding
// transaction commands.
type TestQuery string

// Test executes the query and returns an error if it failed.
func (tq TestQuery) Test(name string, client *QueryClient) error {
	_, err := exec(client, string(tq), nil)
	if err != nil {
		if name == "" {
			return err
		}
		return vterrors.Wrapf(err, "%s: Execute failed", name)
	}
	return nil
}

// Benchmark executes the query and discards the results
func (tq TestQuery) Benchmark(client *QueryClient) error {
	_, err := exec(client, string(tq), nil)
	return err
}

// TestCase represents one test case. It will execute the
// query and verify its results and effects against what
// must be expected. Expected fields are optional.
type TestCase struct {
	// Name gives a name to the test case. It will be used
	// for reporting failures.
	Name string

	// Query and BindVars are the input.
	Query    string
	BindVars map[string]*querypb.BindVariable

	// Result is the list of rows that must be returned.
	// It's represented as 2-d strings. They byte values
	// will be compared against The bytes returned by the
	// query. The check is skipped if Result is nil.
	Result [][]string

	// RowsAffected affected can be nil or an int.
	RowsAffected interface{}

	// 	RowsReturned affected can be nil or an int.
	RowsReturned interface{}

	// Rewritten specifies how the query should have be rewritten.
	Rewritten []string

	// Plan specifies the plan type that was used. It will be matched
	// against tabletserver.PlanType(val).String().
	Plan string

	// If Table is specified, then the framework will validate the
	// cache stats for that table. If the stat values are nil, then
	// the check is skipped.
	Table         string
	Hits          interface{}
	Misses        interface{}
	Absent        interface{}
	Invalidations interface{}
}

// Benchmark executes the test case and discards the results without verifying them
func (tc *TestCase) Benchmark(client *QueryClient) error {
	_, err := exec(client, tc.Query, tc.BindVars)
	return err
}

// Test executes the test case and returns an error if it failed.
// The name parameter is used if the test case doesn't have a name.
func (tc *TestCase) Test(name string, client *QueryClient) error {
	var errs []string
	if tc.Name != "" {
		name = tc.Name
	}

	// wait for all previous test cases to have been settled in cache
	client.server.QueryPlanCacheWait()

	catcher := NewQueryCatcher()
	defer catcher.Close()

	qr, err := exec(client, tc.Query, tc.BindVars)
	if err != nil {
		return vterrors.Wrapf(err, "%s: Execute failed", name)
	}

	if tc.Result != nil {
		result := RowsToStrings(qr)
		if !reflect.DeepEqual(result, tc.Result) {
			errs = append(errs, fmt.Sprintf("Result mismatch:\n'%+v' does not match\n'%+v'", result, tc.Result))
		}
	}

	if tc.RowsAffected != nil {
		want := tc.RowsAffected.(int)
		if int(qr.RowsAffected) != want {
			errs = append(errs, fmt.Sprintf("RowsAffected mismatch: %d, want %d", int(qr.RowsAffected), want))
		}
	}

	if tc.RowsReturned != nil {
		want := tc.RowsReturned.(int)
		if len(qr.Rows) != want {
			errs = append(errs, fmt.Sprintf("RowsReturned mismatch: %d, want %d", len(qr.Rows), want))
		}
	}

	queryInfo, err := catcher.Next()
	if err != nil {
		errs = append(errs, fmt.Sprintf("Query catcher failed: %v", err))
	}
	if tc.Rewritten != nil {
		// Work-around for a quirk. The stream comments also contain
		// "; ". So, we have to get rid of the additional artifacts
		// to make it match expected results.
		unstripped := strings.Split(queryInfo.RewrittenSQL(), "; ")
		got := make([]string, 0, len(unstripped))
		for _, str := range unstripped {
			if str == "" || str == "*/" {
				continue
			}
			got = append(got, str)
		}
		if !reflect.DeepEqual(got, tc.Rewritten) {
			errs = append(errs, fmt.Sprintf("Rewritten mismatch:\n'%q' does not match\n'%q'", got, tc.Rewritten))
		}
	}
	if tc.Plan != "" {
		if queryInfo.PlanType != tc.Plan {
			errs = append(errs, fmt.Sprintf("Plan mismatch: %s, want %s", queryInfo.PlanType, tc.Plan))
		}
	}
	if len(errs) != 0 {
		if name == "" {
			return errors.New(strings.Join(errs, "\n"))
		}
		return errors.New(fmt.Sprintf("%s failed:\n", name) + strings.Join(errs, "\n"))
	}
	return nil
}

func exec(client *QueryClient, query string, bv map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	switch query {
	case "begin":
		return nil, client.Begin(false)
	case "commit":
		return nil, client.Commit()
	case "rollback":
		return nil, client.Rollback()
	}
	return client.Execute(query, bv)
}

// RowsToStrings converts qr.Rows to [][]string.
func RowsToStrings(qr *sqltypes.Result) [][]string {
	var result [][]string
	for _, row := range qr.Rows {
		var srow []string
		for _, cell := range row {
			srow = append(srow, cell.ToString())
		}
		result = append(result, srow)
	}
	return result
}

// MultiCase groups a number of test cases under a name.
// A MultiCase is also Testable. So, it can be recursive.
type MultiCase struct {
	Name  string
	Cases []Testable
}

// Test executes the test cases in MultiCase. The test is
// interrupted if there is a failure. The name parameter is
// used if MultiCase doesn't have a Name.
func (mc *MultiCase) Test(name string, client *QueryClient) error {
	if mc.Name != "" {
		name = mc.Name
	}
	for _, tcase := range mc.Cases {
		if err := tcase.Test(name, client); err != nil {
			client.Rollback()
			return err
		}
	}
	return nil
}

// Benchmark executes the test cases in MultiCase and discards the
// results without validating them.
func (mc *MultiCase) Benchmark(client *QueryClient) error {
	for _, tcase := range mc.Cases {
		if err := tcase.Benchmark(client); err != nil {
			client.Rollback()
			return err
		}
	}
	return nil
}
