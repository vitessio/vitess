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

package sqlparser

import (
	"strconv"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// BindVars is a set of reserved bind variables from a SQL statement
type BindVars map[string]struct{}

// Normalize changes the statement to use bind values, and
// updates the bind vars to those values. The supplied prefix
// is used to generate the bind var names. The function ensures
// that there are no collisions with existing bind vars.
// Within Select constructs, bind vars are deduped. This allows
// us to identify vindex equality. Otherwise, every value is
// treated as distinct.
func Normalize(stmt Statement, known BindVars, bindVars map[string]*querypb.BindVariable, prefix string) error {
	nz := newNormalizer(known, bindVars, prefix)
	_ = Rewrite(stmt, nz.WalkStatement, nil)
	return nz.err
}

type normalizer struct {
	bindVars map[string]*querypb.BindVariable
	prefix   string
	reserved map[string]struct{}
	counter  int
	vals     map[string]string
	err      error
}

func newNormalizer(reserved map[string]struct{}, bindVars map[string]*querypb.BindVariable, prefix string) *normalizer {
	return &normalizer{
		bindVars: bindVars,
		prefix:   prefix,
		reserved: reserved,
		counter:  1,
		vals:     make(map[string]string),
	}
}

// WalkStatement is the top level walk function.
// If it encounters a Select, it switches to a mode
// where variables are deduped.
func (nz *normalizer) WalkStatement(cursor *Cursor) bool {
	switch node := cursor.Node().(type) {
	// no need to normalize the statement types
	case *Set, *Show, *Begin, *Commit, *Rollback, *Savepoint, *SetTransaction, DDLStatement, *SRollback, *Release, *OtherAdmin, *OtherRead:
		return false
	case *Select:
		_ = Rewrite(node, nz.WalkSelect, nil)
		// Don't continue
		return false
	case *Literal:
		nz.convertLiteral(node, cursor)
	case *ComparisonExpr:
		nz.convertComparison(node)
	case *ColName, TableName:
		// Common node types that never contain Literal or ListArgs but create a lot of object
		// allocations.
		return false
	case *ConvertType: // we should not rewrite the type description
		return false
	}
	return nz.err == nil // only continue if we haven't found any errors
}

// WalkSelect normalizes the AST in Select mode.
func (nz *normalizer) WalkSelect(cursor *Cursor) bool {
	switch node := cursor.Node().(type) {
	case *Literal:
		nz.convertLiteralDedup(node, cursor)
	case *ComparisonExpr:
		nz.convertComparison(node)
	case *ColName, TableName:
		// Common node types that never contain Literals or ListArgs but create a lot of object
		// allocations.
		return false
	case OrderBy, GroupBy:
		// do not make a bind var for order by column_position
		return false
	case *ConvertType:
		// we should not rewrite the type description
		return false
	}
	return nz.err == nil // only continue if we haven't found any errors
}

func (nz *normalizer) convertLiteralDedup(node *Literal, cursor *Cursor) {
	// If value is too long, don't dedup.
	// Such values are most likely not for vindexes.
	// We save a lot of CPU because we avoid building
	// the key for them.
	if len(node.Val) > 256 {
		nz.convertLiteral(node, cursor)
		return
	}

	// Make the bindvar
	bval := nz.sqlToBindvar(node)
	if bval == nil {
		return
	}

	// Check if there's a bindvar for that value already.
	var key string
	if bval.Type == sqltypes.VarBinary {
		// Prefixing strings with "'" ensures that a string
		// and number that have the same representation don't
		// collide.
		key = "'" + node.Val
	} else {
		key = node.Val
	}
	bvname, ok := nz.vals[key]
	if !ok {
		// If there's no such bindvar, make a new one.
		bvname = nz.newName()
		nz.vals[key] = bvname
		nz.bindVars[bvname] = bval
	}

	// Modify the AST node to a bindvar.
	cursor.Replace(NewArgument(":" + bvname))
}

// convertLiteral converts an Literal without the dedup.
func (nz *normalizer) convertLiteral(node *Literal, cursor *Cursor) {
	bval := nz.sqlToBindvar(node)
	if bval == nil {
		return
	}

	bvname := nz.newName()
	nz.bindVars[bvname] = bval

	cursor.Replace(NewArgument(":" + bvname))
}

// convertComparison attempts to convert IN clauses to
// use the list bind var construct. If it fails, it returns
// with no change made. The walk function will then continue
// and iterate on converting each individual value into separate
// bind vars.
func (nz *normalizer) convertComparison(node *ComparisonExpr) {
	if node.Operator != InOp && node.Operator != NotInOp {
		return
	}
	tupleVals, ok := node.Right.(ValTuple)
	if !ok {
		return
	}
	// The RHS is a tuple of values.
	// Make a list bindvar.
	bvals := &querypb.BindVariable{
		Type: querypb.Type_TUPLE,
	}
	for _, val := range tupleVals {
		bval := nz.sqlToBindvar(val)
		if bval == nil {
			return
		}
		bvals.Values = append(bvals.Values, &querypb.Value{
			Type:  bval.Type,
			Value: bval.Value,
		})
	}
	bvname := nz.newName()
	nz.bindVars[bvname] = bvals
	// Modify RHS to be a list bindvar.
	node.Right = ListArg(append([]byte("::"), bvname...))
}

func (nz *normalizer) sqlToBindvar(node SQLNode) *querypb.BindVariable {
	if node, ok := node.(*Literal); ok {
		var v sqltypes.Value
		var err error
		switch node.Type {
		case StrVal:
			v, err = sqltypes.NewValue(sqltypes.VarBinary, node.Bytes())
		case IntVal:
			v, err = sqltypes.NewValue(sqltypes.Int64, node.Bytes())
		case FloatVal:
			v, err = sqltypes.NewValue(sqltypes.Float64, node.Bytes())
		default:
			return nil
		}
		if err != nil {
			return nil
		}
		return sqltypes.ValueBindVariable(v)
	}
	return nil
}

func (nz *normalizer) newName() string {
	for {
		newName := nz.prefix + strconv.Itoa(nz.counter)
		if _, ok := nz.reserved[newName]; !ok {
			nz.reserved[newName] = struct{}{}
			return newName
		}
		nz.counter++
	}
}

// GetBindvars returns a map of the bind vars referenced in the statement.
func GetBindvars(stmt Statement) map[string]struct{} {
	bindvars := make(map[string]struct{})
	_ = Walk(func(node SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *ColName, TableName:
			// Common node types that never contain expressions but create a lot of object
			// allocations.
			return false, nil
		case Argument:
			bindvars[string(node[1:])] = struct{}{}
		case ListArg:
			bindvars[string(node[2:])] = struct{}{}
		}
		return true, nil
	}, stmt)
	return bindvars
}
