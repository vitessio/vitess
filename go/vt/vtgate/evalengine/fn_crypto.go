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
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type builtinMD5 struct {
	CallExpr
	collate collations.ID
}

var _ Expr = (*builtinMD5)(nil)

func (call *builtinMD5) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	b := evalToBinary(arg)
	sum := md5.Sum(b.bytes)
	buf := make([]byte, hex.EncodedLen(len(sum)))
	hex.Encode(buf, sum[:])
	return newEvalText(buf, defaultCoercionCollation(call.collate)), nil
}

func (call *builtinMD5) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, t := call.Arguments[0].typeof(env, fields)
	return sqltypes.VarChar, t
}

type builtinSHA1 struct {
	CallExpr
	collate collations.ID
}

var _ Expr = (*builtinSHA1)(nil)

func (call *builtinSHA1) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		return nil, nil
	}

	b := evalToBinary(arg)
	sum := sha1.Sum(b.bytes)
	buf := make([]byte, hex.EncodedLen(len(sum)))
	hex.Encode(buf, sum[:])
	return newEvalText(buf, defaultCoercionCollation(call.collate)), nil
}

func (call *builtinSHA1) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, t := call.Arguments[0].typeof(env, fields)
	return sqltypes.VarChar, t
}

type builtinSHA2 struct {
	CallExpr
	collate collations.ID
}

var _ Expr = (*builtinSHA2)(nil)

func (call *builtinSHA2) eval(env *ExpressionEnv) (eval, error) {
	arg1, arg2, err := call.arg2(env)
	if err != nil {
		return nil, err
	}

	if arg1 == nil || arg2 == nil {
		return nil, nil
	}

	b := evalToBinary(arg1)
	bits := evalToInt64(arg2)

	var sum []byte
	switch bits.i {
	case 224:
		s := sha256.Sum224(b.bytes)
		sum = s[:]
	case 0, 256:
		s := sha256.Sum256(b.bytes)
		sum = s[:]
	case 384:
		s := sha512.Sum384(b.bytes)
		sum = s[:]
	case 512:
		s := sha512.Sum512(b.bytes)
		sum = s[:]
	default:
		return nil, nil
	}

	buf := make([]byte, hex.EncodedLen(len(sum)))
	hex.Encode(buf, sum[:])
	return newEvalText(buf, defaultCoercionCollation(call.collate)), nil
}

func (call *builtinSHA2) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, t := call.Arguments[0].typeof(env, fields)
	return sqltypes.VarChar, t
}

type builtinRandomBytes struct {
	CallExpr
}

var _ Expr = (*builtinRandomBytes)(nil)

func (call *builtinRandomBytes) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}

	if arg == nil {
		return nil, nil
	}

	l := evalToInt64(arg)
	if l.i < 1 || l.i > 1024 {
		return nil, nil
	}

	buf := make([]byte, l.i)
	_, err = rand.Read(buf)
	if err != nil {
		return nil, err
	}
	return newEvalBinary(buf), nil
}

func (call *builtinRandomBytes) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, t := call.Arguments[0].typeof(env, fields)
	return sqltypes.VarBinary, t
}

func (call *builtinRandomBytes) constant() bool {
	return false
}
