package planbuilder

import (
	"fmt"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildStreamPlan(stmt *sqlparser.Stream, vschema ContextVSchema) (engine.Primitive, error) {
	table, _, destTabletType, dest, err := vschema.FindTable(stmt.Table)
	if err != nil {
		return nil, err
	}
	if destTabletType != topodatapb.TabletType_MASTER {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "stream is supported only for master tablet type, current type: %v", destTabletType)
	}
	if dest == nil {
		dest = key.DestinationExactKeyRange{}
	}
	return &engine.MessageStream{
		Keyspace:          table.Keyspace,
		TargetDestination: dest,
		TableName:         table.Name.CompliantName(),
	}, nil
}

func buildVStreamPlan(stmt *sqlparser.VStream, vschema ContextVSchema) (engine.Primitive, error) {
	table, _, _, dest, err := vschema.FindTable(stmt.Table)
	if err != nil {
		return nil, err
	}
	if dest == nil {
		dest = key.DestinationAllShards{}
	}
	limit := 100
	var pos string
	if stmt.Where != nil {
		pos, err = getVStreamStartPos(stmt)
		if err != nil {
			return nil, err
		}
	}
	if stmt.Limit != nil {
		count, ok := stmt.Limit.Rowcount.(*sqlparser.Literal)
		if ok {
			limit, _ = strconv.Atoi(count.Val)
		}
	}

	return &engine.VStream{
		Keyspace:          table.Keyspace,
		TargetDestination: dest,
		TableName:         table.Name.CompliantName(),
		Position:          pos,
		Limit:             limit,
	}, nil
}

func getVStreamStartPos(stmt *sqlparser.VStream) (string, error) {
	var colName, pos string
	if stmt.Where != nil {
		switch v := stmt.Where.Expr.(type) {
		case *sqlparser.ComparisonExpr:
			if v.Operator == sqlparser.GreaterThanOp {
				switch c := v.Left.(type) {
				case *sqlparser.ColName:
					switch val := v.Right.(type) {
					case *sqlparser.Literal:
						pos = string(val.Val)
					}
					colName = strings.ToLower(c.Name.String())
					if colName != "pos" {
						return "", fmt.Errorf("can only use pos in vstream where clause ")
					}
				}
			} else {
				return "", fmt.Errorf("where can only be of type 'pos > <value>'")
			}
		default:
			return "", fmt.Errorf("where can only be of type 'pos > <value>'")
		}
	}
	return pos, nil
}
