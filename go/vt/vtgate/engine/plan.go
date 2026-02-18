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
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vthash"
)

type (
	PlanType int8

	// Plan represents the execution strategy for a given query in Vitess. It is
	// primarily a wrapper around Primitives (the execution instructions). Each
	// Primitive may form a subtree, combining results from its children to
	// achieve the overall query result.
	Plan struct {
		Type         PlanType                // Type of plan (Passthrough, Scatter, JoinOp, Complex, etc.)
		QueryType    sqlparser.StatementType // QueryType indicates the SQL statement type (SELECT, UPDATE, etc.)
		Original     string                  // Original holds the raw query text
		Instructions Primitive               // Instructions define how the query is executed.
		BindVarNeeds *sqlparser.BindVarNeeds // BindVarNeeds lists required bind vars discovered during planning.
		Warnings     []*query.QueryWarning   // Warnings accumulates any warnings generated for this plan.
		TablesUsed   []string                // TablesUsed enumerates the tables this query accesses.
		QueryHints   sqlparser.QueryHints    // QueryHints stores any SET_VAR hints that influenced plan generation.
		ParamsCount  uint16                  // ParamsCount is the total number of bind parameters (?) in the query.
		Optimized    atomic.Bool             // Prepared queries need to be optimized before the first execution

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
		CurrentKeyspace string                // CurrentKeyspace is the name of the keyspace associated with the plan.
		TabletType      topodatapb.TabletType // TabletType is the type of tablet (primary, replica, etc.) for the plan.
		Destination     string                // Destination specifies the shard or routing destination for the plan.
		Query           string                // Query is the original or normalized SQL statement used to build the plan.
		SetVarComment   string                // SetVarComment holds any embedded SET_VAR hints within the query.
		Collation       collations.ID         // Collation is the character collation ID that governs string comparison.
	}
)

const (
	PlanUnknown PlanType = iota
	PlanLocal
	PlanPassthrough
	PlanMultiShard
	PlanLookup
	PlanScatter
	PlanJoinOp
	PlanForeignKey
	PlanComplex
	PlanOnlineDDL
	PlanDirectDDL
	PlanTransaction
	PlanTopoOp
)

func higher(a, b PlanType) PlanType {
	if a > b {
		return a
	}
	return b
}

func (p PlanType) String() string {
	switch p {
	case PlanLocal:
		return "Local"
	case PlanPassthrough:
		return "Passthrough"
	case PlanMultiShard:
		return "MultiShard"
	case PlanScatter:
		return "Scatter"
	case PlanLookup:
		return "Lookup"
	case PlanJoinOp:
		return "Join"
	case PlanForeignKey:
		return "ForeignKey"
	case PlanComplex:
		return "Complex"
	case PlanOnlineDDL:
		return "OnlineDDL"
	case PlanDirectDDL:
		return "DirectDDL"
	case PlanTransaction:
		return "Transaction"
	case PlanTopoOp:
		return "Topology"
	default:
		return "Unknown"
	}
}

func getPlanType(p Primitive) PlanType {
	switch prim := p.(type) {
	case *SessionPrimitive, *SingleRow, *UpdateTarget, *VindexFunc:
		return PlanLocal
	case *Lock, *ReplaceVariables, *RevertMigration, *Rows:
		return PlanPassthrough
	case *Send:
		return getPlanTypeFromTarget(prim)
	case *TransactionStatus:
		return PlanMultiShard
	case *Limit:
		return getPlanType(prim.Input)
	case *VindexLookup:
		return PlanLookup
	case *Join:
		return PlanJoinOp
	case *FkCascade, *FkVerify:
		return PlanForeignKey
	case *Route:
		return getPlanTypeFromRoutingParams(prim.RoutingParameters)
	case *Update:
		pt := getPlanTypeFromRoutingParams(prim.RoutingParameters)
		if prim.isVindexModified() {
			return higher(pt, PlanMultiShard)
		}
		return pt
	case *Delete:
		pt := getPlanTypeFromRoutingParams(prim.RoutingParameters)
		if prim.isVindexModified() {
			return higher(pt, PlanMultiShard)
		}
		return pt
	case *Insert:
		if prim.Opcode == InsertUnsharded {
			return PlanPassthrough
		}
		return PlanMultiShard
	case *Upsert:
		return getPlanTypeForUpsert(prim)
	case *DDL:
		if prim.isOnlineSchemaDDL() {
			return PlanOnlineDDL
		}
		return PlanDirectDDL
	case *ShowExec:
		if prim.Command == sqlparser.VitessVariables {
			return PlanTopoOp
		}
		return PlanLocal
	case *Set:
		return getPlanTypeForSet(prim)
	case *VExplain:
		return getPlanType(prim.Input)
	case *RenameFields:
		return getPlanType(prim.Input)
	default:
		return PlanComplex
	}
}

func getPlanTypeForSet(prim *Set) (pt PlanType) {
	for _, op := range prim.Ops {
		pt = higher(pt, getPlanTypeForSetOp(op))
	}
	return pt
}

func getPlanTypeForSetOp(op SetOp) PlanType {
	switch op.(type) {
	case *UserDefinedVariable, *SysVarIgnore, *SysVarSetAware:
		return PlanLocal
	case *SysVarCheckAndIgnore:
		return PlanPassthrough
	case *SysVarReservedConn:
		return PlanMultiShard
	case *VitessMetadata:
		return PlanTopoOp
	default:
		return PlanUnknown
	}
}

func getPlanTypeFromTarget(prim *Send) PlanType {
	switch prim.TargetDestination.(type) {
	case key.DestinationNone:
		return PlanLocal
	case key.DestinationAnyShard, key.DestinationShard, key.DestinationKeyspaceID:
		return PlanPassthrough
	case key.DestinationAllShards:
		return PlanScatter
	default:
		return PlanMultiShard
	}
}

func getPlanTypeFromRoutingParams(rp *RoutingParameters) PlanType {
	if rp == nil {
		panic("RoutingParameters is nil, cannot determine plan type")
	}
	switch rp.Opcode {
	case Unsharded, Next, DBA, Reference:
		return PlanPassthrough
	case EqualUnique:
		if rp.Vindex != nil && rp.Vindex.NeedsVCursor() {
			return PlanLookup
		}
		return PlanPassthrough
	case Equal, IN, Between, MultiEqual, SubShard, ByDestination:
		if rp.Vindex != nil && rp.Vindex.NeedsVCursor() {
			return PlanLookup
		}
		return PlanMultiShard
	case Scatter:
		return PlanScatter
	case None:
		return PlanLocal
	}
	panic("cannot determine plan type for the given opcode: " + rp.Opcode.String())
}

func getPlanTypeForUpsert(prim *Upsert) PlanType {
	var finalPlanType PlanType
	for _, u := range prim.Upserts {
		finalPlanType = higher(finalPlanType, getPlanType(u.Update))
		finalPlanType = higher(finalPlanType, getPlanType(u.Insert))
	}
	return finalPlanType
}

func (pk PlanKey) DebugString() string {
	return fmt.Sprintf("CurrentKeyspace: %s, TabletType: %s, Destination: %s, Query: %s, SetVarComment: %s, Collation: %d", pk.CurrentKeyspace, pk.TabletType.String(), pk.Destination, pk.Query, pk.SetVarComment, pk.Collation)
}

func (pk PlanKey) Hash() theine.HashKey256 {
	hasher := vthash.New256()
	_, _ = hasher.WriteUint16(uint16(pk.Collation))
	_, _ = hasher.WriteUint16(uint16(pk.TabletType))
	_, _ = hasher.WriteString(pk.CurrentKeyspace)
	_, _ = hasher.WriteString(pk.Destination)
	_, _ = hasher.WriteString(pk.SetVarComment)
	_, _ = hasher.WriteString(pk.Query)

	var planKey theine.HashKey256
	hasher.Sum(planKey[:0])
	return planKey
}

func NewPlan(query string, stmt sqlparser.Statement, primitive Primitive, bindVarNeeds *sqlparser.BindVarNeeds, tablesUsed []string) *Plan {
	return &Plan{
		Type:         getPlanType(primitive),
		QueryType:    sqlparser.ASTToStatementType(stmt),
		Original:     query,
		Instructions: primitive,
		BindVarNeeds: bindVarNeeds,
		TablesUsed:   tablesUsed,
	}
}

// MarshalJSON serializes the plan into a JSON representation.
func (p *Plan) MarshalJSON() ([]byte, error) {
	var instructions *PrimitiveDescription
	if p.Instructions != nil {
		description := PrimitiveToPlanDescription(p.Instructions, nil)
		instructions = &description
	}

	marshalPlan := struct {
		Type         string
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
		Type:         p.Type.String(),
		QueryType:    p.QueryType.String(),
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
