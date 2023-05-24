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
	"encoding/binary"
	"math"
	"net/netip"

	"github.com/google/uuid"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	builtinInetAton struct {
		CallExpr
	}

	builtinInetNtoa struct {
		CallExpr
		collate collations.ID
	}

	builtinInet6Aton struct {
		CallExpr
	}

	builtinInet6Ntoa struct {
		CallExpr
		collate collations.ID
	}

	builtinIsIPV4 struct {
		CallExpr
	}

	builtinIsIPV4Compat struct {
		CallExpr
	}

	builtinIsIPV4Mapped struct {
		CallExpr
	}

	builtinIsIPV6 struct {
		CallExpr
	}

	builtinBinToUUID struct {
		CallExpr
		collate collations.ID
	}

	builtinIsUUID struct {
		CallExpr
	}

	builtinUUID struct {
		CallExpr
	}

	builtinUUIDToBin struct {
		CallExpr
	}
)

var _ Expr = (*builtinInetAton)(nil)
var _ Expr = (*builtinInetNtoa)(nil)
var _ Expr = (*builtinInet6Aton)(nil)
var _ Expr = (*builtinInet6Ntoa)(nil)
var _ Expr = (*builtinIsIPV4)(nil)
var _ Expr = (*builtinIsIPV4Compat)(nil)
var _ Expr = (*builtinIsIPV4Mapped)(nil)
var _ Expr = (*builtinIsIPV6)(nil)
var _ Expr = (*builtinBinToUUID)(nil)
var _ Expr = (*builtinIsUUID)(nil)
var _ Expr = (*builtinUUID)(nil)
var _ Expr = (*builtinUUIDToBin)(nil)

func (call *builtinInetAton) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if arg == nil || err != nil {
		return nil, err
	}
	rawIp := evalToBinary(arg)
	ip, err := netip.ParseAddr(rawIp.string())
	if err != nil || !ip.Is4() {
		return nil, nil
	}
	return newEvalUint64(uint64(binary.BigEndian.Uint32(ip.AsSlice()))), nil
}

func (call *builtinInetAton) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Uint64, flagNullable
}

func (call *builtinInetAton) compile(c *compiler) (ctype, error) {
	str, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xb(1, sqltypes.VarBinary, 0, false)
	}

	c.asm.Fn_INET_ATON()
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.Uint64, Flag: flagNullable, Col: collationNumeric}, nil
}

func (call *builtinInetNtoa) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if arg == nil || err != nil {
		return nil, err
	}
	rawIp := uint64(evalToInt64(arg).i)

	if rawIp > math.MaxUint32 {
		return nil, nil
	}

	b := binary.BigEndian.AppendUint32(nil, uint32(rawIp))
	return newEvalText(hack.StringBytes(netip.AddrFrom4([4]byte(b)).String()), defaultCoercionCollation(call.collate)), nil
}

func (call *builtinInetNtoa) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, t := call.Arguments[0].typeof(env, fields)
	return sqltypes.VarChar, t | flagNullable
}

func (call *builtinInetNtoa) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	c.compileToUint64(arg, 1)
	col := defaultCoercionCollation(call.collate)
	c.asm.Fn_INET_NTOA(col)
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.VarChar, Flag: flagNullable, Col: defaultCoercionCollation(call.collate)}, nil
}

func (call *builtinInet6Aton) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if arg == nil || err != nil {
		return nil, err
	}
	rawIp := evalToBinary(arg)
	ip, err := netip.ParseAddr(rawIp.string())
	if err != nil {
		return nil, nil
	}
	b := ip.AsSlice()
	return newEvalBinary(b), nil
}

func (call *builtinInet6Aton) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.VarBinary, flagNullable
}

func (call *builtinInet6Aton) compile(c *compiler) (ctype, error) {
	str, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(str)

	switch {
	case str.isTextual():
	default:
		c.asm.Convert_xb(1, sqltypes.VarBinary, 0, false)
	}

	c.asm.Fn_INET6_ATON()
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.VarBinary, Flag: flagNullable, Col: collationBinary}, nil
}

func printIPv6AsIPv4(addr netip.Addr) (netip.Addr, bool) {
	b := addr.AsSlice()
	if len(b) != 16 {
		return addr, false
	}
	for i := 0; i < 12; i++ {
		if b[i] != 0 {
			return addr, false
		}
	}
	if b[12] == 0 && b[13] == 0 {
		return addr, false
	}
	return netip.AddrFrom4(([4]byte)(b[12:])), true
}

func isIPv4Compat(addr netip.Addr) bool {
	b := addr.AsSlice()
	if len(b) != 16 {
		return false
	}
	for i := 0; i < 12; i++ {
		if b[i] != 0 {
			return false
		}
	}
	if b[12] == 0 && b[13] == 0 && b[14] == 0 && b[15] < 2 {
		return false
	}
	return true
}

func (call *builtinInet6Ntoa) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if arg == nil || err != nil {
		return nil, err
	}
	b, ok := arg.(*evalBytes)
	if !ok || !b.isBinary() {
		return nil, nil
	}

	ip, ok := netip.AddrFromSlice(b.bytes)
	if !ok {
		return nil, nil
	}

	if ip, ok := printIPv6AsIPv4(ip); ok {
		return newEvalText(hack.StringBytes("::"+ip.String()), defaultCoercionCollation(call.collate)), nil
	}

	return newEvalText(hack.StringBytes(ip.String()), defaultCoercionCollation(call.collate)), nil
}

func (call *builtinInet6Ntoa) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.VarChar, flagNullable
}

func (call *builtinInet6Ntoa) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.VarBinary, sqltypes.Blob, sqltypes.Binary:
		col := defaultCoercionCollation(call.collate)
		c.asm.Fn_INET6_NTOA(col)
	default:
		c.asm.SetNull(1)
	}
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.VarChar, Flag: flagNullable, Col: defaultCoercionCollation(call.collate)}, nil
}

func (call *builtinIsIPV4) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if arg == nil || err != nil {
		return nil, err
	}
	rawIp := evalToBinary(arg)
	ip, err := netip.ParseAddr(rawIp.string())
	if err != nil {
		return newEvalBool(false), nil
	}
	return newEvalBool(ip.Is4()), nil
}

func (call *builtinIsIPV4) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, t := call.Arguments[0].typeof(env, fields)
	return sqltypes.Int64, t
}

func (call *builtinIsIPV4) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip := c.compileNullCheck1(arg)

	switch {
	case arg.isTextual():
	default:
		c.asm.Convert_xb(1, sqltypes.VarBinary, 0, false)
	}

	c.asm.Fn_IS_IPV4()
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.Int64, Flag: arg.Flag | flagIsBoolean, Col: collationNumeric}, nil
}

func (call *builtinIsIPV4Compat) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if arg == nil || err != nil {
		return nil, err
	}
	b, ok := arg.(*evalBytes)
	if !ok || !b.isBinary() {
		return newEvalBool(false), nil
	}

	ip, ok := netip.AddrFromSlice(b.bytes)
	return newEvalBool(ok && isIPv4Compat(ip)), nil
}

func (call *builtinIsIPV4Compat) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Int64, flagIsBoolean
}

func (call *builtinIsIPV4Compat) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip := c.compileNullCheck1(arg)

	switch arg.Type {
	case sqltypes.VarBinary, sqltypes.Blob, sqltypes.Binary:
		c.asm.Fn_IS_IPV4_COMPAT()
	default:
		c.asm.SetBool(1, false)
	}
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Flag: arg.Flag | flagIsBoolean, Col: collationNumeric}, nil
}

func (call *builtinIsIPV4Mapped) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if arg == nil || err != nil {
		return nil, err
	}
	b, ok := arg.(*evalBytes)
	if !ok || !b.isBinary() {
		return newEvalBool(false), nil
	}

	ip, ok := netip.AddrFromSlice(b.bytes)
	return newEvalBool(ok && ip.Is4In6()), nil
}

func (call *builtinIsIPV4Mapped) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Int64, flagIsBoolean
}

func (call *builtinIsIPV4Mapped) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)
	switch arg.Type {
	case sqltypes.VarBinary, sqltypes.Blob, sqltypes.Binary:
		c.asm.Fn_IS_IPV4_MAPPED()
	default:
		c.asm.SetBool(1, false)
	}
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Flag: arg.Flag | flagIsBoolean, Col: collationNumeric}, nil
}

func (call *builtinIsIPV6) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if arg == nil || err != nil {
		return nil, err
	}
	rawIp := evalToBinary(arg)
	ip, err := netip.ParseAddr(rawIp.string())
	if err != nil {
		return newEvalBool(false), nil
	}
	return newEvalBool(ip.Is6()), nil
}

func (call *builtinIsIPV6) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.Int64, flagIsBoolean
}

func (call *builtinIsIPV6) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip := c.compileNullCheck1(arg)

	switch {
	case arg.isTextual():
	default:
		c.asm.Convert_xb(1, sqltypes.VarBinary, 0, false)
	}

	c.asm.Fn_IS_IPV6()
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.Int64, Flag: arg.Flag | flagIsBoolean, Col: collationNumeric}, nil
}

func errIncorrectUUID(in []byte, f string) error {
	return vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongValue, "Incorrect string value: '%s' for function %s", sanitizeErrorValue(in), f)
}

func swapUUIDFrom(in []byte) []byte {
	if len(in) != 16 {
		return in
	}
	out := make([]byte, 0, 16)
	out = append(out, in[4:8]...)
	out = append(out, in[2:4]...)
	out = append(out, in[0:2]...)
	out = append(out, in[8:]...)
	return out
}

func swapUUIDTo(in []byte) []byte {
	if len(in) != 16 {
		return in
	}

	out := make([]byte, 0, 16)
	out = append(out, in[6:8]...)
	out = append(out, in[4:6]...)
	out = append(out, in[0:4]...)
	out = append(out, in[8:]...)
	return out
}

func (call *builtinBinToUUID) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if arg == nil || err != nil {
		return nil, err
	}

	raw := evalToBinary(arg).bytes

	if len(call.Arguments) > 1 {
		swap, err := call.Arguments[1].eval(env)
		if err != nil {
			return nil, err
		}

		if swap != nil && evalToInt64(swap).i != 0 {
			raw = swapUUIDFrom(raw)
		}
	}

	parsed, err := uuid.FromBytes(raw)
	if err != nil {
		return nil, errIncorrectUUID(raw, "bin_to_uuid")
	}
	return newEvalText(hack.StringBytes(parsed.String()), defaultCoercionCollation(call.collate)), nil
}

func (call *builtinBinToUUID) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.VarChar, f
}

func (call *builtinBinToUUID) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip := c.compileNullCheck1(arg)

	switch {
	case arg.isTextual():
	default:
		c.asm.Convert_xb(1, sqltypes.VarBinary, 0, false)
	}

	col := defaultCoercionCollation(call.collate)
	ct := ctype{Type: sqltypes.VarChar, Flag: arg.Flag, Col: col}

	if len(call.Arguments) == 1 {
		c.asm.Fn_BIN_TO_UUID0(col)
		c.asm.jumpDestination(skip)
		return ct, nil
	}

	swap, err := call.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}

	sj := c.compileNullCheck1(swap)
	switch swap.Type {
	case sqltypes.Int64:
	case sqltypes.Uint64:
		c.asm.Convert_ui(1)
	default:
		c.asm.Convert_xi(1)
	}
	c.asm.jumpDestination(sj)
	c.asm.Fn_BIN_TO_UUID1(col)

	c.asm.jumpDestination(skip)
	return ct, nil
}

func (call *builtinIsUUID) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if arg == nil || err != nil {
		return nil, err
	}

	raw := evalToBinary(arg).bytes
	_, err = uuid.ParseBytes(raw)
	return newEvalBool(err == nil), nil
}

func (call *builtinIsUUID) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.Int64, f | flagIsBoolean
}

func (call *builtinIsUUID) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip := c.compileNullCheck1(arg)

	switch {
	case arg.isTextual():
	default:
		c.asm.Convert_xb(1, sqltypes.VarBinary, 0, false)
	}
	c.asm.Fn_IS_UUID()

	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Flag: arg.Flag | flagIsBoolean, Col: collationNumeric}, nil
}

func (call *builtinUUID) eval(env *ExpressionEnv) (eval, error) {
	v, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	m, err := v.MarshalText()
	if err != nil {
		return nil, err
	}

	return newEvalText(m, collationUtf8mb3), nil
}

func (call *builtinUUID) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	return sqltypes.VarChar, 0
}

func (call *builtinUUID) compile(c *compiler) (ctype, error) {
	c.asm.Fn_UUID()
	return ctype{Type: sqltypes.VarChar, Flag: 0, Col: collationUtf8mb3}, nil
}

func (call *builtinUUIDToBin) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if arg == nil || err != nil {
		return nil, err
	}

	raw := evalToBinary(arg).bytes

	parsed, err := uuid.ParseBytes(raw)
	if err != nil {
		return nil, errIncorrectUUID(raw, "uuid_to_bin")
	}

	out := parsed[:]
	if len(call.Arguments) > 1 {
		swap, err := call.Arguments[1].eval(env)
		if err != nil {
			return nil, err
		}

		if swap != nil && evalToInt64(swap).i != 0 {
			out = swapUUIDTo(out)
		}
	}

	return newEvalBinary(out), nil
}

func (call *builtinUUIDToBin) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f := call.Arguments[0].typeof(env, fields)
	return sqltypes.VarBinary, f
}

func (call *builtinUUIDToBin) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip := c.compileNullCheck1(arg)

	switch {
	case arg.isTextual():
	default:
		c.asm.Convert_xb(1, sqltypes.VarBinary, 0, false)
	}

	ct := ctype{Type: sqltypes.VarBinary, Flag: arg.Flag, Col: collationBinary}

	if len(call.Arguments) == 1 {
		c.asm.Fn_UUID_TO_BIN0()
		c.asm.jumpDestination(skip)
		return ct, nil
	}

	swap, err := call.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}

	sj := c.compileNullCheck1(swap)
	switch swap.Type {
	case sqltypes.Int64:
	case sqltypes.Uint64:
		c.asm.Convert_ui(1)
	default:
		c.asm.Convert_xi(1)
	}
	c.asm.jumpDestination(sj)
	c.asm.Fn_UUID_TO_BIN1()

	c.asm.jumpDestination(skip)
	return ct, nil
}
