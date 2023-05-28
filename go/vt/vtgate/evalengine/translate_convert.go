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
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/decimal"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

func (ast *astCompiler) binaryCollationForCollation(collation collations.ID) collations.ID {
	binary := collation.Get()
	if binary == nil {
		return collations.Unknown
	}
	binaryCollation := collations.Local().BinaryCollationForCharset(binary.Charset().Name())
	if binaryCollation == nil {
		return collations.Unknown
	}
	return binaryCollation.ID()
}

func (ast *astCompiler) translateConvertCharset(charset string, binary bool) (collations.ID, error) {
	if charset == "" {
		collation := ast.cfg.Collation
		if binary {
			collation = ast.binaryCollationForCollation(collation)
		}
		if collation == collations.Unknown {
			return collations.Unknown, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No default character set specified")
		}
		return collation, nil
	}
	charset = strings.ToLower(charset)
	collation := collations.Local().DefaultCollationForCharset(charset)
	if collation == nil {
		return collations.Unknown, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Unknown character set: '%s'", charset)
	}
	collationID := collation.ID()
	if binary {
		collationID = ast.binaryCollationForCollation(collationID)
		if collationID == collations.Unknown {
			return collations.Unknown, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No binary collation found for character set: %s ", charset)
		}
	}
	return collationID, nil
}

func (ast *astCompiler) translateConvertExpr(expr sqlparser.Expr, convertType *sqlparser.ConvertType) (Expr, error) {
	var (
		convert ConvertExpr
		err     error
	)

	convert.Inner, err = ast.translateExpr(expr)
	if err != nil {
		return nil, err
	}

	convert.Length, convert.HasLength, err = ast.translateIntegral(convertType.Length)
	if err != nil {
		return nil, err
	}

	convert.Scale, convert.HasScale, err = ast.translateIntegral(convertType.Scale)
	if err != nil {
		return nil, err
	}

	convert.Type = strings.ToUpper(convertType.Type)
	switch convert.Type {
	case "DECIMAL":
		if convert.Length < convert.Scale {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
				"For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '%s').",
				"", // TODO: column name
			)
		}
		if convert.Length > decimal.MyMaxPrecision {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
				"Too-big precision %d specified for '%s'. Maximum is %d.",
				convert.Length, sqlparser.String(expr), decimal.MyMaxPrecision)
		}
		if convert.Scale > decimal.MyMaxScale {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
				"Too big scale %d specified for column '%s'. Maximum is %d.",
				convert.Scale, sqlparser.String(expr), decimal.MyMaxScale)
		}
	case "NCHAR":
		convert.Collation = collations.CollationUtf8ID
	case "CHAR":
		convert.Collation, err = ast.translateConvertCharset(convertType.Charset.Name, convertType.Charset.Binary)
		if err != nil {
			return nil, err
		}
	case "BINARY", "DOUBLE", "REAL", "SIGNED", "SIGNED INTEGER", "UNSIGNED", "UNSIGNED INTEGER", "JSON", "TIME", "DATETIME", "DATE":
		// Supported types for conv expression
	default:
		// For unsupported types, we should return an error on translation instead of returning an error on runtime.
		return nil, convert.returnUnsupportedError()
	}

	return &convert, nil
}

func (ast *astCompiler) translateConvertUsingExpr(expr *sqlparser.ConvertUsingExpr) (Expr, error) {
	var (
		using ConvertUsingExpr
		err   error
	)

	using.Inner, err = ast.translateExpr(expr.Expr)
	if err != nil {
		return nil, err
	}

	using.Collation, err = ast.translateConvertCharset(expr.Type, false)
	if err != nil {
		return nil, err
	}

	return &using, nil
}
