package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// FKVerify represents an insert operation on a table.
type FKVerify struct {
	// Sources are used to run the validation queries.
	Sources      []ops.Operator
	InputOffsets [][]int

	// ParentFKs to verify during insert planning
	ParentFKs []vindexes.ParentFKInfo

	noColumns
	noPredicates
}

func (fkVerify *FKVerify) Inputs() []ops.Operator {
	return fkVerify.Sources
}

func (fkVerify *FKVerify) SetInputs(operators []ops.Operator) {
	fkVerify.Sources = operators
}

func (fkVerify *FKVerify) ShortDescription() string {
	return "FKVerify"
}

func (fkVerify *FKVerify) GetOrdering() ([]ops.OrderBy, error) {
	panic("does not expect insert operator to receive get ordering call")
}

// Clone will return a copy of this operator, protected so changed to the original will not impact the clone
func (fkVerify *FKVerify) Clone(inputs []ops.Operator) ops.Operator {
	return &FKVerify{
		Sources: inputs,
	}
}

// NewFkVerify creates a new FkVerify operator given the parent foreign key validations we want it to run.
func NewFkVerify(ctx *plancontext.PlanningContext, parentFKs []vindexes.ParentFKInfo, insertColumns sqlparser.Columns) (*FKVerify, error) {
	var verifyQueryOps []ops.Operator
	var inputOffsets [][]int
	for _, fk := range parentFKs {
		// Build the query for verification
		query := &sqlparser.Select{
			SelectExprs: []sqlparser.SelectExpr{
				sqlparser.NewAliasedExpr(
					&sqlparser.Count{
						Args: createColNameList(fk.ParentColumns),
					},
					"",
				),
			},
			From: []sqlparser.TableExpr{
				sqlparser.NewAliasedTableExpr(sqlparser.NewTableName(fk.Table.Name.String()), ""),
			},
			Where: sqlparser.NewWhere(
				sqlparser.WhereClause,
				sqlparser.NewComparisonExpr(
					sqlparser.InOp,
					createLeftExpr(fk.ParentColumns),
					sqlparser.NewListArg("fkInput"), // TODO: Make this a constant in the engine package.
					nil,
				),
			),
		}

		newSemTable, err := semantics.Analyze(query, fk.Table.Keyspace.Name, ctx.VSchema)
		if err != nil {
			return nil, err
		}
		newCtx := plancontext.NewPlanningContext(nil, newSemTable, ctx.VSchema, ctx.PlannerVersion)
		op, err := PlanQuery(newCtx, query)
		if err != nil {
			return nil, err
		}
		verifyQueryOps = append(verifyQueryOps, op)

		// Build the offsets
		var offsets []int
	outer:
		for _, column := range fk.ChildColumns {
			for idx, insertColumn := range insertColumns {
				if column.Equal(insertColumn) {
					offsets = append(offsets, idx)
					continue outer
				}
			}
			offsets = append(offsets, -1)
		}
		inputOffsets = append(inputOffsets, offsets)
	}

	return &FKVerify{
		Sources:      verifyQueryOps,
		InputOffsets: inputOffsets,
	}, nil
}

// createColNameList creates a list of ColNames from a list of Columns
func createColNameList(columns sqlparser.Columns) []sqlparser.Expr {
	var colNames []sqlparser.Expr
	for _, col := range columns {
		colNames = append(colNames, sqlparser.NewColName(col.String()))
	}
	return colNames
}

// createLeftExpr creates a valtuple if there are more than 1 columns, otherwise it just returns a ColName.
func createLeftExpr(columns sqlparser.Columns) sqlparser.Expr {
	valTuple := createColNameList(columns)
	if len(valTuple) == 1 {
		return valTuple[0]
	}
	return sqlparser.ValTuple(valTuple)
}
