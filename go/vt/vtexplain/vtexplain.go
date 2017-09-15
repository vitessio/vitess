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
	"fmt"
	"sort"
	"strings"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/jsonutil"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vtgate/engine"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
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
}

// TabletQuery defines a query that was sent to a given tablet and how it was
// processed in mysql
type TabletQuery struct {
	// SQL command sent to the given tablet
	SQL string

	// BindVars sent with the command
	BindVars map[string]*querypb.BindVariable
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
		SQL      string
		BindVars map[string]string
	}{
		SQL:      tq.SQL,
		BindVars: bindVars,
	})
}

// TabletActions contains the set of operations done by a given tablet
type TabletActions struct {
	// Queries sent from vtgate to the tablet
	TabletQueries []*TabletQuery

	// Queries that were run on mysql
	MysqlQueries []string
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

	parsedDDLs, err := parseSchema(sqlSchema)
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

func parseSchema(sqlSchema string) ([]*sqlparser.DDL, error) {
	parsedDDLs := make([]*sqlparser.DDL, 0, 16)
	for _, sql := range strings.Split(sqlSchema, ";") {
		s := sqlparser.StripLeadingComments(sql)
		s, _ = sqlparser.SplitTrailingComments(sql)
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}

		stmt, err := sqlparser.Parse(sql)
		if err != nil {
			log.Errorf("ERROR: failed to parse sql: %s, got error: %v", sql, err)
			continue
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
		if ddl.TableSpec == nil {
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
		rem := ""
		idx := strings.Index(sql, ";")
		if idx != -1 {
			rem = sql[idx+1:]
			sql = sql[:idx]
		}

		if sql != "" {
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

// ExplainsAsText returns a text representation of the explains
func ExplainsAsText(explains []*Explain) string {
	var b bytes.Buffer
	for _, explain := range explains {
		fmt.Fprintf(&b, "----------------------------------------------------------------------\n")
		fmt.Fprintf(&b, "%s\n\n", explain.SQL)

		tablets := make([]string, 0, len(explain.TabletActions))
		for tablet := range explain.TabletActions {
			tablets = append(tablets, tablet)
		}
		sort.Strings(tablets)
		for _, tablet := range tablets {
			fmt.Fprintf(&b, "[%s]:\n", tablet)
			tc := explain.TabletActions[tablet]
			for _, sql := range tc.MysqlQueries {
				fmt.Fprintf(&b, "%s\n", sql)
			}
			fmt.Fprintf(&b, "\n")
		}
	}
	fmt.Fprintf(&b, "----------------------------------------------------------------------\n")
	return string(b.Bytes())
}

// ExplainsAsJSON returns a json representation of the explains
func ExplainsAsJSON(explains []*Explain) string {
	explainJSON, _ := jsonutil.MarshalIndentNoEscape(explains, "", "    ")
	return string(explainJSON)
}
