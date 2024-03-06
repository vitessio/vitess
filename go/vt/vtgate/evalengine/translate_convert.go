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
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/mysql/decimal"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

func (ast *astCompiler) binaryCollationForCollation(collation collations.ID) collations.ID {
	binary := colldata.Lookup(collation)
	if binary == nil {
		return collations.Unknown
	}
	return ast.cfg.Environment.CollationEnv().BinaryCollationForCharset(binary.Charset().Name())
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
	collationID := ast.cfg.Environment.CollationEnv().DefaultCollationForCharset(charset)
	if collationID == collations.Unknown {
		return collations.Unknown, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Unknown character set: '%s'", charset)
	}
	if binary {
		collationID = ast.binaryCollationForCollation(collationID)
		if collationID == collations.Unknown {
			return collations.Unknown, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "No binary collation found for character set: %s ", charset)
		}
	}
	return collationID, nil
}

func (ast *astCompiler) translateConvertExpr(expr sqlparser.Expr, convertType *sqlparser.ConvertType) (IR, error) {
	var (
		convert ConvertExpr
		err     error
	)

	convert.CollationEnv = ast.cfg.Environment.CollationEnv()
	convert.Inner, err = ast.translateExpr(expr)
	if err != nil {
		return nil, err
	}

	convert.Length = convertType.Length
	convert.Scale = convertType.Scale
	convert.Type = strings.ToUpper(convertType.Type)
	switch convert.Type {
	case "DECIMAL":
		if convert.Length != nil {
			if *convert.Length > decimal.MyMaxPrecision {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
					"Too-big precision %d specified for '%s'. Maximum is %d.",
					*convert.Length, sqlparser.String(expr), decimal.MyMaxPrecision)
			}
			if convert.Scale != nil {
				if *convert.Scale > decimal.MyMaxScale {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
						"Too big scale %d specified for column '%s'. Maximum is %d.",
						*convert.Scale, sqlparser.String(expr), decimal.MyMaxScale)
				}
				if *convert.Length < *convert.Scale {
					return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
						"For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '%s').",
						"", // TODO: column name
					)
				}
			}
		}
	case "NCHAR":
		convert.Collation = collations.CollationUtf8mb3ID
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

func (ast *astCompiler) translateConvertUsingExpr(expr *sqlparser.ConvertUsingExpr) (IR, error) {
	var (
		using ConvertUsingExpr
		err   error
	)

	using.CollationEnv = ast.cfg.Environment.CollationEnv()
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
