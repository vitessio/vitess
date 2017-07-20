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
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func Example() {
	// 1. Create a SplitParams object.
	// There are two "constructors": NewSplitParamsGivenSplitCount and
	// NewSplitParamsGivenNumRowsPerQueryPart. They each take several parameters including a "schema"
	// object which should be a map[string]*schema.Table that maps a table name to its schema.Table
	// object. It is used for error-checking the split columns and their types. We use an empty
	// object for this toy example, but in real code this object must have correct entries.
	//
	// This schema can is typically derived from tabletserver.TabletServer.qe.se.
	schema := map[string]*schema.Table{}
	splitParams, err := NewSplitParamsGivenSplitCount(
		&querypb.BoundQuery{
			Sql:           "SELECT * FROM table WHERE id > :id",
			BindVariables: map[string]*querypb.BindVariable{"id": sqltypes.Int64BindVariable(5)},
		},
		[]sqlparser.ColIdent{
			sqlparser.NewColIdent("id"),
			sqlparser.NewColIdent("user_id"),
		}, // SplitColumns
		1000, // SplitCount
		schema)
	if err != nil {
		panic(fmt.Sprintf("NewSplitParamsGivenSplitCount failed with: %v", err))
	}

	// 2. Create the SplitAlgorithmInterface object used for splitting.
	// SplitQuery supports multiple algorithms for splitting the query. These are encapsulated as
	// types implementing the SplitAlgorithmInterface. Currently two algorithms are supported
	// represented by the FullScanAlgorithm and EqualSplitsAlgorithm types. See the documentation
	// of these types for more details on each algorithm.
	// To do the split we'll need to create an object of one of these types and pass it to the
	// Splitter (see below). Here we use the FullScan algorithm.
	// We also pass a type implementing the SQLExecuter interface that the algorithm will
	// use to send statements to MySQL.
	algorithm, err := NewFullScanAlgorithm(splitParams, getSQLExecuter())
	if err != nil {
		panic(fmt.Sprintf("NewFullScanAlgorithm failed with: %v", err))
	}

	// 3. Create a splitter object. Always succeeds.
	splitter := NewSplitter(splitParams, algorithm)

	// 4. Call splitter.Split() to Split the query.
	// The result is a slice of &querypb.QuerySplit objects (and an error object).
	queryParts, err := splitter.Split()
	if err != nil {
		panic(fmt.Sprintf("splitter.Split() failed with: %v", err))
	}
	fmt.Println(queryParts)
}

func getSQLExecuter() SQLExecuter {
	// In real code, this should be an object implementing the SQLExecuter interface.
	return nil
}
