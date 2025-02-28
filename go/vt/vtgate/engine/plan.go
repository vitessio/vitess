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
	"fmt"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/cache/theine"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vthash"
)

type (
	// Plan represents the execution strategy for a given query in Vitess. It is
	// primarily a wrapper around Primitives (the execution instructions). Each
	// Primitive may form a subtree, combining results from its children to
	// achieve the overall query result.
	Plan struct {
		Type         sqlparser.StatementType // Type indicates the SQL statement type (SELECT, UPDATE, etc.).
		Original     string                  // Original holds the raw query text
		Instructions Primitive               // Instructions define how the query is executed.
		BindVarNeeds *sqlparser.BindVarNeeds // BindVarNeeds lists required bind vars discovered during planning.
		Warnings     []*query.QueryWarning   // Warnings accumulates any warnings generated for this plan.
		TablesUsed   []string                // TablesUsed enumerates the tables this query accesses.
		QueryHints   sqlparser.QueryHints    // QueryHints stores any SET_VAR hints that influenced plan generation.
		ParamsCount  uint16                  // ParamsCount is the total number of bind parameters (?) in the query.

		ExecCount    uint64 // ExecCount is how many times this plan has been executed.
		ExecTime     uint64 // ExecTime is the total accumulated execution time in nanoseconds.
		ShardQueries uint64 // ShardQueries is the total count of shard-level queries performed.
		RowsReturned uint64 // RowsReturned is the total number of rows returned to clients.
		RowsAffected uint64 // RowsAffected is the total number of rows affected by DML operations.
		Errors       uint64 // Errors is the total count of errors encountered during execution.
	}

	// PlanKey identifies a plan uniquely based on keyspace, destination, query,
	// SET_VAR comment, and collation. It is primarily used as a cache key.
	PlanKey struct {
		CurrentKeyspace string        // CurrentKeyspace is the name of the keyspace associated with the plan.
		Destination     string        // Destination specifies the shard or routing destination for the plan.
		Query           string        // Query is the original or normalized SQL statement used to build the plan.
		SetVarComment   string        // SetVarComment holds any embedded SET_VAR hints within the query.
		Collation       collations.ID // Collation is the character collation ID that governs string comparison.
	}
)

func (pk PlanKey) DebugString() string {
	return fmt.Sprintf("CurrentKeyspace: %s, Destination: %s, Query: %s, SetVarComment: %s, Collation: %d", pk.CurrentKeyspace, pk.Destination, pk.Query, pk.SetVarComment, pk.Collation)
}

func (pk PlanKey) Hash() theine.HashKey256 {
	hasher := vthash.New256()
	_, _ = hasher.WriteUint16(uint16(pk.Collation))
	_, _ = hasher.WriteString(pk.CurrentKeyspace)
	_, _ = hasher.WriteString(pk.Destination)
	_, _ = hasher.WriteString(pk.SetVarComment)
	_, _ = hasher.WriteString(pk.Query)

	var planKey theine.HashKey256
	hasher.Sum(planKey[:0])
	return planKey
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
		description := PrimitiveToPlanDescription(p.Instructions, nil)
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
