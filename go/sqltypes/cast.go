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
