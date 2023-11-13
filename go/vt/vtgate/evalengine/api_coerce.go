/*
Copyright 2023 The Vitess Authors.

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

package evalengine

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func CoerceTo(value sqltypes.Value, typ sqltypes.Type) (sqltypes.Value, error) {
	cast, err := valueToEvalCast(value, value.Type(), collations.Unknown)
	if err != nil {
		return sqltypes.Value{}, err
	}
	return evalToSQLValueWithType(cast, typ), nil
}

// CoerceTypes takes two input types, and decides how they should be coerced before compared
func CoerceTypes(v1, v2 Type) (out Type, err error) {
	if v1 == v2 {
		return v1, nil
	}
	if sqltypes.IsNull(v1.Type) || sqltypes.IsNull(v2.Type) {
		return Type{Type: sqltypes.Null, Coll: collations.CollationBinaryID, Nullable: true}, nil
	}

	nullable := v1.Nullable || v2.Nullable
	if (sqltypes.IsText(v1.Type) || sqltypes.IsBinary(v1.Type)) && (sqltypes.IsText(v2.Type) || sqltypes.IsBinary(v2.Type)) {
		mergedCollation, _, _, err := mergeCollations(typedCoercionCollation(v1.Type, v1.Coll), typedCoercionCollation(v2.Type, v2.Coll), v1.Type, v2.Type)
		if err != nil {
			return Type{}, err
		}
		return Type{Type: sqltypes.VarChar, Coll: mergedCollation.Collation, Nullable: nullable}, nil
	}
	if sqltypes.IsNumber(v1.Type) || sqltypes.IsNumber(v2.Type) {
		switch {
		case sqltypes.IsText(v1.Type) || sqltypes.IsBinary(v1.Type) || sqltypes.IsText(v2.Type) || sqltypes.IsBinary(v2.Type):
			return Type{Type: sqltypes.Float64, Coll: collations.CollationBinaryID, Nullable: nullable}, nil
		case sqltypes.IsFloat(v2.Type) || v2.Type == sqltypes.Decimal || sqltypes.IsFloat(v1.Type) || v1.Type == sqltypes.Decimal:
			return Type{Type: sqltypes.Float64, Coll: collations.CollationBinaryID, Nullable: nullable}, nil
		case sqltypes.IsSigned(v1.Type):
			switch {
			case sqltypes.IsUnsigned(v2.Type):
				return Type{Type: sqltypes.Uint64, Coll: collations.CollationBinaryID, Nullable: nullable}, nil
			case sqltypes.IsSigned(v2.Type):
				return Type{Type: sqltypes.Int64, Coll: collations.CollationBinaryID, Nullable: nullable}, nil
			default:
				return Type{}, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "types does not support hashcode yet: %v vs %v", v1, v2.Type)
			}
		case sqltypes.IsUnsigned(v1.Type):
			switch {
			case sqltypes.IsSigned(v2.Type) || sqltypes.IsUnsigned(v2.Type):
				return Type{Type: sqltypes.Uint64, Coll: collations.CollationBinaryID, Nullable: nullable}, nil
			default:
				return Type{}, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "types does not support hashcode yet: %v vs %v", v1, v2)
			}
		}
	}
	return Type{}, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "types does not support hashcode yet: %v vs %v", v1, v2)
}
