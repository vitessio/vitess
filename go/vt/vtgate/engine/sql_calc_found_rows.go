package engine

import (
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

var _ Primitive = (*SQLCalcFoundRows)(nil)

// SQLCalcFoundRows is a primitive to execute limit and count query as per their individual plan.
type SQLCalcFoundRows struct {
	LimitPrimitive Primitive
	CountPrimitive Primitive
}

// RouteType implements the Primitive interface
func (s SQLCalcFoundRows) RouteType() string {
	return "SQLCalcFoundRows"
}

// GetKeyspaceName implements the Primitive interface
func (s SQLCalcFoundRows) GetKeyspaceName() string {
	return s.LimitPrimitive.GetKeyspaceName()
}

// GetTableName implements the Primitive interface
func (s SQLCalcFoundRows) GetTableName() string {
	return s.LimitPrimitive.GetTableName()
}

// TryExecute implements the Primitive interface
func (s SQLCalcFoundRows) TryExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	limitQr, err := vcursor.ExecutePrimitive(s.LimitPrimitive, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	countQr, err := vcursor.ExecutePrimitive(s.CountPrimitive, bindVars, false)
	if err != nil {
		return nil, err
	}
	if len(countQr.Rows) != 1 || len(countQr.Rows[0]) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "count query is not a scalar")
	}
	fr, err := evalengine.ToUint64(countQr.Rows[0][0])
	if err != nil {
		return nil, err
	}
	vcursor.Session().SetFoundRows(fr)
	return limitQr, nil
}

// TryStreamExecute implements the Primitive interface
func (s SQLCalcFoundRows) TryStreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	err := vcursor.StreamExecutePrimitive(s.LimitPrimitive, bindVars, wantfields, callback)
	if err != nil {
		return err
	}

	var fr *uint64

	err = vcursor.StreamExecutePrimitive(s.CountPrimitive, bindVars, wantfields, func(countQr *sqltypes.Result) error {
		if len(countQr.Rows) == 0 && countQr.Fields != nil {
			// this is the fields, which we can ignore
			return nil
		}
		if len(countQr.Rows) != 1 || len(countQr.Rows[0]) != 1 {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "count query is not a scalar")
		}
		toUint64, err := evalengine.ToUint64(countQr.Rows[0][0])
		if err != nil {
			return err
		}
		fr = &toUint64
		return nil
	})
	if err != nil {
		return err
	}
	if fr == nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "count query for SQL_CALC_FOUND_ROWS never returned a value")
	}
	vcursor.Session().SetFoundRows(*fr)
	return nil
}

// GetFields implements the Primitive interface
func (s SQLCalcFoundRows) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return s.LimitPrimitive.GetFields(vcursor, bindVars)
}

// NeedsTransaction implements the Primitive interface
func (s SQLCalcFoundRows) NeedsTransaction() bool {
	return s.LimitPrimitive.NeedsTransaction()
}

// Inputs implements the Primitive interface
func (s SQLCalcFoundRows) Inputs() []Primitive {
	return []Primitive{s.LimitPrimitive, s.CountPrimitive}
}

func (s SQLCalcFoundRows) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "SQL_CALC_FOUND_ROWS",
	}
}
