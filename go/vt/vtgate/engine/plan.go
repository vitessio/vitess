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

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

// Plan represents the execution strategy for a given query.
// For now it's a simple wrapper around the real instructions.
// An instruction (aka Primitive) is typically a tree where
// each node does its part by combining the results of the
// sub-nodes.
type (
	PlanType int8

	Plan struct {
		Type         PlanType                // Type of plan
		QueryType    sqlparser.StatementType // Type of query
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
	panic(fmt.Sprintf("cannot determine plan type for the given opcode: %s", rp.Opcode.String()))
}

func getPlanTypeForUpsert(prim *Upsert) PlanType {
	var finalPlanType PlanType
	for _, u := range prim.Upserts {
		finalPlanType = higher(finalPlanType, getPlanType(u.Update))
		finalPlanType = higher(finalPlanType, getPlanType(u.Insert))
	}
	return finalPlanType
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
