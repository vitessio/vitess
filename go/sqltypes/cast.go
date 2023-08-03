package sqltypes

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// Cast converts a Value to the target type.
func Cast(v Value, typ Type) (Value, error) {
	if v.Type() == typ || v.IsNull() {
		return v, nil
	}
	vBytes, err := v.ToBytes()
	if err != nil {
		return v, err
	}
	if IsSigned(typ) && v.IsSigned() {
		return MakeTrusted(typ, vBytes), nil
	}
	if IsUnsigned(typ) && v.IsUnsigned() {
		return MakeTrusted(typ, vBytes), nil
	}
	if (IsFloat(typ) || typ == Decimal) && (v.IsIntegral() || v.IsFloat() || v.Type() == Decimal) {
		return MakeTrusted(typ, vBytes), nil
	}
	if IsQuoted(typ) && (v.IsIntegral() || v.IsFloat() || v.Type() == Decimal || v.IsQuoted()) {
		return MakeTrusted(typ, vBytes), nil
	}

	// Explicitly disallow Expression.
	if v.Type() == Expression {
		return NULL, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v cannot be cast to %v", v, typ)
	}

	// If the above fast-paths were not possible,
	// go through full validation.
	return NewValue(typ, vBytes)
}
