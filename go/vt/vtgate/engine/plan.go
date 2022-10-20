/*
Copyright 2022 The Vitess Authors.

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
	"bytes"
	"encoding/json"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

// Plan represents the execution strategy for a given query.
// For now it's a simple wrapper around the real instructions.
// An instruction (aka Primitive) is typically a tree where
// each node does its part by combining the results of the
// sub-nodes.
type Plan struct {
	Type         sqlparser.StatementType // The type of query we have
	Original     string                  // Original is the original query.
	Instructions Primitive               // Instructions contains the instructions needed to fulfil the query.
	BindVarNeeds *sqlparser.BindVarNeeds // Stores BindVars needed to be provided as part of expression rewriting
	Warnings     []*query.QueryWarning   // Warnings that need to be yielded every time this query runs
	TablesUsed   []string                // TablesUsed is the list of tables that this plan will query

	ExecCount    uint64 // Count of times this plan was executed
	ExecTime     uint64 // Total execution time
	ShardQueries uint64 // Total number of shard queries
	RowsReturned uint64 // Total number of rows
	RowsAffected uint64 // Total number of rows
	Errors       uint64 // Total number of errors
}

// AddStats updates the plan execution statistics
func (p *Plan) AddStats(execCount uint64, execTime time.Duration, shardQueries, rowsAffected, rowsReturned, errors uint64) {
	atomic.AddUint64(&p.ExecCount, execCount)
	atomic.AddUint64(&p.ExecTime, uint64(execTime))
	atomic.AddUint64(&p.ShardQueries, shardQueries)
	atomic.AddUint64(&p.RowsAffected, rowsAffected)
	atomic.AddUint64(&p.RowsReturned, rowsReturned)
	atomic.AddUint64(&p.Errors, errors)
}

// Stats returns a copy of the plan execution statistics
func (p *Plan) Stats() (execCount uint64, execTime time.Duration, shardQueries, rowsAffected, rowsReturned, errors uint64) {
	execCount = atomic.LoadUint64(&p.ExecCount)
	execTime = time.Duration(atomic.LoadUint64(&p.ExecTime))
	shardQueries = atomic.LoadUint64(&p.ShardQueries)
	rowsAffected = atomic.LoadUint64(&p.RowsAffected)
	rowsReturned = atomic.LoadUint64(&p.RowsReturned)
	errors = atomic.LoadUint64(&p.Errors)
	return
}

// MarshalJSON serializes the plan into a JSON representation.
func (p *Plan) MarshalJSON() ([]byte, error) {
	var instructions *PrimitiveDescription
	if p.Instructions != nil {
		description := PrimitiveToPlanDescription(p.Instructions)
		instructions = &description
	}

	marshalPlan := struct {
		QueryType    string
		Original     string                `json:",omitempty"`
		Instructions *PrimitiveDescription `json:",omitempty"`
		ExecCount    uint64                `json:",omitempty"`
		ExecTime     time.Duration         `json:",omitempty"`
		ShardQueries uint64                `json:",omitempty"`
		RowsAffected uint64                `json:",omitempty"`
		RowsReturned uint64                `json:",omitempty"`
		Errors       uint64                `json:",omitempty"`
		TablesUsed   []string              `json:",omitempty"`
	}{
		QueryType:    p.Type.String(),
		Original:     p.Original,
		Instructions: instructions,
		ExecCount:    atomic.LoadUint64(&p.ExecCount),
		ExecTime:     time.Duration(atomic.LoadUint64(&p.ExecTime)),
		ShardQueries: atomic.LoadUint64(&p.ShardQueries),
		RowsAffected: atomic.LoadUint64(&p.RowsAffected),
		RowsReturned: atomic.LoadUint64(&p.RowsReturned),
		Errors:       atomic.LoadUint64(&p.Errors),
		TablesUsed:   p.TablesUsed,
	}

	b := new(bytes.Buffer)
	enc := json.NewEncoder(b)
	enc.SetEscapeHTML(false)
	err := enc.Encode(marshalPlan)
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}
