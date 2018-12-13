/*
Copyright 2017 Google Inc.

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

// Package vtexplain analyzes a set of sql statements and returns the
// corresponding vtgate and vttablet query plans that will be executed
// on the given statements
package vtexplain

import (
	"bytes"
	"flag"
	"fmt"
	"sort"
	"time"

	"vitess.io/vitess/go/jsonutil"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var (
	batchInterval = flag.Duration("batch-interval", 10*time.Millisecond, "Interval between logical time slots.")
)

// ExecutorMode controls the mode of operation for the vtexplain simulator
type ExecutorMode string

const (
	// ModeMulti is the default mode with autocommit implemented at vtgate
	ModeMulti = "multi"

	// ModeTwoPC enables the twopc feature
	ModeTwoPC = "twopc"
)

// Options to control the explain process
type Options struct {
	// NumShards indicates the number of shards in the topology
	NumShards int

	// ReplicationMode must be set to either "ROW" or "STATEMENT" before
	// initialization
	ReplicationMode string

	// Normalize controls whether or not vtgate does query normalization
	Normalize bool

	// ExecutionMode must be set to one of the modes above
	ExecutionMode string

	// StrictDDL is used in unit tests only to verify that the schema
	// is parsed properly.
	StrictDDL bool

	// Target is used to override the "database" target in the
	// vtgate session to simulate `USE <target>`
	Target string
}

// TabletQuery defines a query that was sent to a given tablet and how it was
// processed in mysql
type TabletQuery struct {
	// Logical time of the query
	Time int

	// SQL command sent to the given tablet
	SQL string

	// BindVars sent with the command
	BindVars map[string]*querypb.BindVariable
}

// MysqlQuery defines a query that was sent to a given tablet and how it was
// processed in mysql
type MysqlQuery struct {
	// Sequence number of the query
	Time int

	// SQL command sent to the given tablet
	SQL string
}

// MarshalJSON renders the json structure
func (tq *TabletQuery) MarshalJSON() ([]byte, error) {
	// Convert Bindvars to strings for nicer output
	bindVars := make(map[string]string)
	for k, v := range tq.BindVars {
		var b bytes.Buffer
		sqlparser.EncodeValue(&b, v)
		bindVars[k] = b.String()
	}

	return jsonutil.MarshalNoEscape(&struct {
		Time     int
		SQL      string
		BindVars map[string]string
	}{
		Time:     tq.Time,
		SQL:      tq.SQL,
		BindVars: bindVars,
	})
}

// TabletActions contains the set of operations done by a given tablet
type TabletActions struct {
	// Queries sent from vtgate to the tablet
	TabletQueries []*TabletQuery

	// Queries that were run on mysql
	MysqlQueries []*MysqlQuery
}

// Explain defines how vitess will execute a given sql query, including the vtgate
// query plans and all queries run on each tablet.
type Explain struct {
	// original sql statement
	SQL string

	// the vtgate plan(s)
	Plans []*engine.Plan

	// list of queries / bind vars sent to each tablet
	TabletActions map[string]*TabletActions
}

const (
	vtexplainCell = "explainCell"
)

// Init sets up the fake execution environment
func Init(vSchemaStr, sqlSchema string, opts *Options) error {
	// Verify options
	if opts.ReplicationMode != "ROW" && opts.ReplicationMode != "STATEMENT" {
		return fmt.Errorf("invalid replication mode \"%s\"", opts.ReplicationMode)
	}

	parsedDDLs, err := parseSchema(sqlSchema, opts)
	if err != nil {
		return fmt.Errorf("parseSchema: %v", err)
	}

	err = initTabletEnvironment(parsedDDLs, opts)
	if err != nil {
		return fmt.Errorf("initTabletEnvironment: %v", err)
	}

	err = initVtgateExecutor(vSchemaStr, opts)
	if err != nil {
		return fmt.Errorf("initVtgateExecutor: %v", err)
	}

	return nil
}

// Stop and cleans up fake execution environment
func Stop() {
	// Cleanup all created fake dbs.
	if explainTopo != nil {
		for _, conn := range explainTopo.TabletConns {
			conn.tsv.StopService()
		}
		for _, conn := range explainTopo.TabletConns {
			conn.db.Close()
		}
	}
}

func parseSchema(sqlSchema string, opts *Options) ([]*sqlparser.DDL, error) {
	parsedDDLs := make([]*sqlparser.DDL, 0, 16)
	for {
		sql, rem, err := sqlparser.SplitStatement(sqlSchema)
		sqlSchema = rem
		if err != nil {
			return nil, err
		}
		if sql == "" {
			break
		}
		sql = sqlparser.StripComments(sql)
		if sql == "" {
			continue
		}

		var stmt sqlparser.Statement
		if opts.StrictDDL {
			stmt, err = sqlparser.ParseStrictDDL(sql)
			if err != nil {
				return nil, err
			}
		} else {
			stmt, err = sqlparser.Parse(sql)
			if err != nil {
				log.Errorf("ERROR: failed to parse sql: %s, got error: %v", sql, err)
				continue
			}
		}
		ddl, ok := stmt.(*sqlparser.DDL)
		if !ok {
			log.Infof("ignoring non-DDL statement: %s", sql)
			continue
		}
		if ddl.Action != sqlparser.CreateStr {
			log.Infof("ignoring %s table statement", ddl.Action)
			continue
		}
		if ddl.TableSpec == nil && ddl.OptLike == nil {
			log.Errorf("invalid create table statement: %s", sql)
			continue
		}
		parsedDDLs = append(parsedDDLs, ddl)
	}
	return parsedDDLs, nil
}

// Run the explain analysis on the given queries
func Run(sql string) ([]*Explain, error) {
	explains := make([]*Explain, 0, 16)

	var (
		rem string
		err error
	)

	for {
		// Need to strip comments in a loop to handle multiple comments
		// in a row.
		for {
			s := sqlparser.StripLeadingComments(sql)
			if s == sql {
				break
			}
			sql = s
		}

		sql, rem, err = sqlparser.SplitStatement(sql)
		if err != nil {
			return nil, err
		}

		if sql != "" {
			// Reset the global time simulator for each query
			batchTime = sync2.NewBatcher(*batchInterval)
			log.V(100).Infof("explain %s", sql)
			e, err := explain(sql)
			if err != nil {
				return nil, err
			}
			explains = append(explains, e)
		}

		sql = rem
		if sql == "" {
			break
		}
	}

	return explains, nil
}

func explain(sql string) (*Explain, error) {
	plans, tabletActions, err := vtgateExecute(sql)
	if err != nil {
		return nil, err
	}

	return &Explain{
		SQL:           sql,
		Plans:         plans,
		TabletActions: tabletActions,
	}, nil
}

type outputQuery struct {
	tablet string
	Time   int
	sql    string
}

// ExplainsAsText returns a text representation of the explains in logical time
// order
func ExplainsAsText(explains []*Explain) string {
	var b bytes.Buffer
	for _, explain := range explains {
		fmt.Fprintf(&b, "----------------------------------------------------------------------\n")
		fmt.Fprintf(&b, "%s\n\n", explain.SQL)

		queries := make([]outputQuery, 0, 4)
		for tablet, actions := range explain.TabletActions {
			for _, q := range actions.MysqlQueries {
				queries = append(queries, outputQuery{
					tablet: tablet,
					Time:   q.Time,
					sql:    q.SQL,
				})
			}
		}

		// Make sure to sort first by the batch time and then by the
		// shard to avoid flakiness in the tests for parallel queries
		sort.SliceStable(queries, func(i, j int) bool {
			if queries[i].Time == queries[j].Time {
				return queries[i].tablet < queries[j].tablet
			}
			return queries[i].Time < queries[j].Time
		})

		for _, q := range queries {
			fmt.Fprintf(&b, "%d %s: %s\n", q.Time, q.tablet, q.sql)
		}
		fmt.Fprintf(&b, "\n")
	}
	fmt.Fprintf(&b, "----------------------------------------------------------------------\n")
	return string(b.Bytes())
}

// ExplainsAsJSON returns a json representation of the explains
func ExplainsAsJSON(explains []*Explain) string {
	explainJSON, _ := jsonutil.MarshalIndentNoEscape(explains, "", "    ")
	return string(explainJSON)
}
