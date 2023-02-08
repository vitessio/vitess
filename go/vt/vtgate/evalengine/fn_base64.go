package evalengine

import (
	"encoding/base64"

	"vitess.io/vitess/go/sqltypes"
)

type (
	builtinToBase64 struct {
		CallExpr
	}

	builtinFromBase64 struct {
		CallExpr
	}
)

var _ Expr = (*builtinToBase64)(nil)
var _ Expr = (*builtinFromBase64)(nil)

var mysqlBase64 = base64.StdEncoding

func (call *builtinToBase64) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	b := evalToBinary(arg)
	encoded := make([]byte, mysqlBase64.EncodedLen(len(b.bytes)))
	mysqlBase64.Encode(encoded, b.bytes)

	if arg.sqlType() == sqltypes.Blob || arg.sqlType() == sqltypes.TypeJSON {
		return newEvalRaw(sqltypes.Text, encoded, env.collation()), nil
	}
	return newEvalText(encoded, env.collation()), nil
}

func (call *builtinToBase64) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	tt, f := call.Arguments[0].typeof(env)
	if tt == sqltypes.Blob || tt == sqltypes.TypeJSON {
		return sqltypes.Text, f
	}
	return sqltypes.VarChar, f
}

func (call *builtinFromBase64) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	b := evalToBinary(arg)
	decoded := make([]byte, mysqlBase64.DecodedLen(len(b.bytes)))
	if n, err := mysqlBase64.Decode(decoded, b.bytes); err == nil {
		return newEvalBinary(decoded[:n]), nil
	}
	return nil, nil
}

func (call *builtinFromBase64) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env)
	return sqltypes.VarBinary, f | flagNullable
}
