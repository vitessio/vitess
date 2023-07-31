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
	"bytes"
	"math/big"

	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/mysql/hex"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

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
func Normalize(stmt Statement, reserved *ReservedVars, bindVars map[string]*querypb.BindVariable) error {
	nz := newNormalizer(reserved, bindVars)
	_ = SafeRewrite(stmt, nz.walkStatementDown, nz.walkStatementUp)
	return nz.err
}

type normalizer struct {
	bindVars  map[string]*querypb.BindVariable
	reserved  *ReservedVars
	vals      map[string]string
	err       error
	inDerived bool
}

func newNormalizer(reserved *ReservedVars, bindVars map[string]*querypb.BindVariable) *normalizer {
	return &normalizer{
		bindVars: bindVars,
		reserved: reserved,
		vals:     make(map[string]string),
	}
}

// walkStatementUp is one half of the top level walk function.
func (nz *normalizer) walkStatementUp(cursor *Cursor) bool {
	if nz.err != nil {
		return false
	}
	node, isLiteral := cursor.Node().(*Literal)
	if !isLiteral {
		return true
	}
	nz.convertLiteral(node, cursor)
	return nz.err == nil // only continue if we haven't found any errors
}

// walkStatementDown is the top level walk function.
// If it encounters a Select, it switches to a mode
// where variables are deduped.
func (nz *normalizer) walkStatementDown(node, parent SQLNode) bool {
	switch node := node.(type) {
	// no need to normalize the statement types
	case *Set, *Show, *Begin, *Commit, *Rollback, *Savepoint, DDLStatement, *SRollback, *Release, *OtherAdmin, *OtherRead:
		return false
	case *Select:
		_, isDerived := parent.(*DerivedTable)
		var tmp bool
		tmp, nz.inDerived = nz.inDerived, isDerived
		_ = SafeRewrite(node, nz.walkDownSelect, nz.walkUpSelect)
		// Don't continue
		nz.inDerived = tmp
		return false
	case *ComparisonExpr:
		nz.convertComparison(node)
	case *UpdateExpr:
		nz.convertUpdateExpr(node)
	case *ColName, TableName:
		// Common node types that never contain Literal or ListArgs but create a lot of object
		// allocations.
		return false
	case *ConvertType: // we should not rewrite the type description
		return false
	}
	return nz.err == nil // only continue if we haven't found any errors
}

// walkDownSelect normalizes the AST in Select mode.
func (nz *normalizer) walkDownSelect(node, parent SQLNode) bool {
	switch node := node.(type) {
	case *Select:
		_, isDerived := parent.(*DerivedTable)
		if !isDerived {
			return true
		}
		var tmp bool
		tmp, nz.inDerived = nz.inDerived, isDerived
		// initiating a new AST walk here means that we might change something while walking down on the tree,
		// but since we are only changing literals, we can be safe that we are not changing the SELECT struct,
		// only something much further down, and that should be safe
		_ = SafeRewrite(node, nz.walkDownSelect, nz.walkUpSelect)
		// Don't continue
		nz.inDerived = tmp
		return false
	case SelectExprs:
		return !nz.inDerived
	case *ComparisonExpr:
		nz.convertComparison(node)
	case *FramePoint:
		// do not make a bind var for rows and range
		return false
	case *ColName, TableName:
		// Common node types that never contain Literals or ListArgs but create a lot of object
		// allocations.
		return false
	case *ConvertType:
		// we should not rewrite the type description
		return false
	}
	return nz.err == nil // only continue if we haven't found any errors
}

// walkUpSelect normalizes the Literals in Select mode.
func (nz *normalizer) walkUpSelect(cursor *Cursor) bool {
	if nz.err != nil {
		return false
	}
	node, isLiteral := cursor.Node().(*Literal)
	if !isLiteral {
		return true
	}
	parent := cursor.Parent()
	switch parent.(type) {
	case *Order, GroupBy:
		return false
	case *Limit:
		nz.convertLiteral(node, cursor)
	default:
		nz.convertLiteralDedup(node, cursor)
	}
	return nz.err == nil // only continue if we haven't found any errors
}

func validateLiteral(node *Literal) error {
	switch node.Type {
	case DateVal:
		if _, ok := datetime.ParseDate(node.Val); !ok {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Incorrect DATE value: '%s'", node.Val)
		}
	case TimeVal:
		if _, _, ok := datetime.ParseTime(node.Val, -1); !ok {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Incorrect TIME value: '%s'", node.Val)
		}
	case TimestampVal:
		if _, _, ok := datetime.ParseDateTime(node.Val, -1); !ok {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Incorrect DATETIME value: '%s'", node.Val)
		}
	}
	return nil
}

func (nz *normalizer) convertLiteralDedup(node *Literal, cursor *Cursor) {
	err := validateLiteral(node)
	if err != nil {
		nz.err = err
	}

	// If value is too long, don't dedup.
	// Such values are most likely not for vindexes.
	// We save a lot of CPU because we avoid building
	// the key for them.
	if len(node.Val) > 256 {
		nz.convertLiteral(node, cursor)
		return
	}

	// Make the bindvar
	bval := SQLToBindvar(node)
	if bval == nil {
		return
	}

	// Check if there's a bindvar for that value already.
	key := keyFor(bval, node)
	bvname, ok := nz.vals[key]
	if !ok {
		// If there's no such bindvar, make a new one.
		bvname = nz.reserved.nextUnusedVar()
		nz.vals[key] = bvname
		nz.bindVars[bvname] = bval
	}

	// Modify the AST node to a bindvar.
	cursor.Replace(NewTypedArgument(bvname, node.SQLType()))
}

func keyFor(bval *querypb.BindVariable, lit *Literal) string {
	if bval.Type != sqltypes.VarBinary && bval.Type != sqltypes.VarChar {
		return lit.Val
	}

	// Prefixing strings with "'" ensures that a string
	// and number that have the same representation don't
	// collide.
	return "'" + lit.Val
}

// convertLiteral converts an Literal without the dedup.
func (nz *normalizer) convertLiteral(node *Literal, cursor *Cursor) {
	err := validateLiteral(node)
	if err != nil {
		nz.err = err
	}

	bval := SQLToBindvar(node)
	if bval == nil {
		return
	}

	bvname := nz.reserved.nextUnusedVar()
	nz.bindVars[bvname] = bval
	cursor.Replace(NewTypedArgument(bvname, node.SQLType()))
}

// convertComparison attempts to convert IN clauses to
// use the list bind var construct. If it fails, it returns
// with no change made. The walk function will then continue
// and iterate on converting each individual value into separate
// bind vars.
func (nz *normalizer) convertComparison(node *ComparisonExpr) {
	switch node.Operator {
	case InOp, NotInOp:
		nz.rewriteInComparisons(node)
	default:
		nz.rewriteOtherComparisons(node)
	}
}

func (nz *normalizer) rewriteOtherComparisons(node *ComparisonExpr) {
	newR := nz.parameterize(node.Left, node.Right)
	if newR != nil {
		node.Right = newR
	}
}

func (nz *normalizer) parameterize(left, right Expr) Expr {
	col, ok := left.(*ColName)
	if !ok {
		return nil
	}
	lit, ok := right.(*Literal)
	if !ok {
		return nil
	}
	err := validateLiteral(lit)
	if err != nil {
		nz.err = err
		return nil
	}

	bval := SQLToBindvar(lit)
	if bval == nil {
		return nil
	}
	key := keyFor(bval, lit)
	bvname := nz.decideBindVarName(key, lit, col, bval)
	return NewTypedArgument(bvname, lit.SQLType())
}

func (nz *normalizer) decideBindVarName(key string, lit *Literal, col *ColName, bval *querypb.BindVariable) string {
	if len(lit.Val) <= 256 {
		// first we check if we already have a bindvar for this value. if we do, we re-use that bindvar name
		bvname, ok := nz.vals[key]
		if ok {
			return bvname
		}
	}

	// If there's no such bindvar, or we have a big value, make a new one.
	// Big values are most likely not for vindexes.
	// We save a lot of CPU because we avoid building
	bvname := nz.reserved.ReserveColName(col)
	nz.vals[key] = bvname
	nz.bindVars[bvname] = bval

	return bvname
}

func (nz *normalizer) rewriteInComparisons(node *ComparisonExpr) {
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
		bval := SQLToBindvar(val)
		if bval == nil {
			return
		}
		bvals.Values = append(bvals.Values, &querypb.Value{
			Type:  bval.Type,
			Value: bval.Value,
		})
	}
	bvname := nz.reserved.nextUnusedVar()
	nz.bindVars[bvname] = bvals
	// Modify RHS to be a list bindvar.
	node.Right = ListArg(bvname)
}

func (nz *normalizer) convertUpdateExpr(node *UpdateExpr) {
	newR := nz.parameterize(node.Name, node.Expr)
	if newR != nil {
		node.Expr = newR
	}
}

func SQLToBindvar(node SQLNode) *querypb.BindVariable {
	if node, ok := node.(*Literal); ok {
		var v sqltypes.Value
		var err error
		switch node.Type {
		case StrVal:
			v, err = sqltypes.NewValue(sqltypes.VarChar, node.Bytes())
		case IntVal:
			v, err = sqltypes.NewValue(sqltypes.Int64, node.Bytes())
		case FloatVal:
			v, err = sqltypes.NewValue(sqltypes.Float64, node.Bytes())
		case DecimalVal:
			v, err = sqltypes.NewValue(sqltypes.Decimal, node.Bytes())
		case HexNum:
			buf := make([]byte, 0, len(node.Bytes()))
			buf = append(buf, "0x"...)
			buf = append(buf, bytes.ToUpper(node.Bytes()[2:])...)
			v, err = sqltypes.NewValue(sqltypes.HexNum, buf)
		case HexVal:
			// We parse the `x'7b7d'` string literal into a hex encoded string of `7b7d` in the parser
			// We need to re-encode it back to the original MySQL query format before passing it on as a bindvar value to MySQL
			buf := make([]byte, 0, len(node.Bytes())+3)
			buf = append(buf, 'x', '\'')
			buf = append(buf, bytes.ToUpper(node.Bytes())...)
			buf = append(buf, '\'')
			v, err = sqltypes.NewValue(sqltypes.HexVal, buf)
		case BitVal:
			// Convert bit value to hex number in parameterized query format
			var i big.Int
			_, ok := i.SetString(string(node.Bytes()), 2)
			if !ok {
				return nil
			}

			buf := i.Bytes()
			out := make([]byte, 0, (len(buf)*2)+2)
			out = append(out, '0', 'x')
			out = append(out, hex.EncodeBytes(buf)...)
			v, err = sqltypes.NewValue(sqltypes.HexNum, out)
		case DateVal:
			v, err = sqltypes.NewValue(sqltypes.Date, node.Bytes())
		case TimeVal:
			v, err = sqltypes.NewValue(sqltypes.Time, node.Bytes())
		case TimestampVal:
			// This is actually a DATETIME MySQL type. The timestamp literal
			// syntax is part of the SQL standard and MySQL DATETIME matches
			// the type best.
			v, err = sqltypes.NewValue(sqltypes.Datetime, node.Bytes())
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

// GetBindvars returns a map of the bind vars referenced in the statement.
func GetBindvars(stmt Statement) map[string]struct{} {
	bindvars := make(map[string]struct{})
	_ = Walk(func(node SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *ColName, TableName:
			// Common node types that never contain expressions but create a lot of object
			// allocations.
			return false, nil
		case *Argument:
			bindvars[node.Name] = struct{}{}
		case ListArg:
			bindvars[string(node)] = struct{}{}
		}
		return true, nil
	}, stmt)
	return bindvars
}
