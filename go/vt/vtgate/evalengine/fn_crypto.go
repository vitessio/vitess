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
)

type builtinMD5 struct {
	CallExpr
	collate collations.ID
}

var _ IR = (*builtinMD5)(nil)

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
	return newEvalText(buf, typedCoercionCollation(sqltypes.VarChar, call.collate)), nil
}

func (call *builtinMD5) compile(c *compiler) (ctype, error) {
	str, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xb(1, sqltypes.Binary, nil)
	}

	col := typedCoercionCollation(sqltypes.VarChar, c.collation)
	c.asm.Fn_MD5(col)
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.VarChar, Col: col, Flag: nullableFlags(str.Flag)}, nil
}

type builtinSHA1 struct {
	CallExpr
	collate collations.ID
}

var _ IR = (*builtinSHA1)(nil)

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
	return newEvalText(buf, typedCoercionCollation(sqltypes.VarChar, call.collate)), nil
}

func (call *builtinSHA1) compile(c *compiler) (ctype, error) {
	str, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xb(1, sqltypes.Binary, nil)
	}
	col := typedCoercionCollation(sqltypes.VarChar, c.collation)
	c.asm.Fn_SHA1(col)
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.VarChar, Col: col, Flag: nullableFlags(str.Flag)}, nil
}

type builtinSHA2 struct {
	CallExpr
	collate collations.ID
}

var _ IR = (*builtinSHA2)(nil)

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
	return newEvalText(buf, typedCoercionCollation(sqltypes.VarChar, call.collate)), nil
}

func (call *builtinSHA2) compile(c *compiler) (ctype, error) {
	str, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip1 := c.compileNullCheck1(str)

	bits, err := call.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip2 := c.compileNullCheck1r(bits)

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xb(2, sqltypes.Binary, nil)
	}

	switch bits.Type {
	case sqltypes.Int64:
		// No-op, already correct type
	case sqltypes.Uint64:
		c.asm.Convert_ui(1)
	default:
		c.asm.Convert_xi(1)
	}

	col := typedCoercionCollation(sqltypes.VarChar, c.collation)
	c.asm.Fn_SHA2(col)
	c.asm.jumpDestination(skip1, skip2)
	return ctype{Type: sqltypes.VarChar, Col: col, Flag: nullableFlags(str.Flag)}, nil
}

type builtinRandomBytes struct {
	CallExpr
}

var _ IR = (*builtinRandomBytes)(nil)

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

func (call *builtinRandomBytes) constant() bool {
	return false
}

func (call *builtinRandomBytes) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.Int64:
		// No-op, already correct type
	case sqltypes.Uint64:
		c.asm.Convert_ui(1)
	default:
		c.asm.Convert_xi(1)
	}

	c.asm.Fn_RandomBytes()
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.VarBinary, Col: collationBinary, Flag: nullableFlags(arg.Flag) | flagNullable}, nil
}
