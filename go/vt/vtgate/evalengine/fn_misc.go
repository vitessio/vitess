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

	builtinLastInsertID struct {
		CallExpr
	}
)

var (
	_ IR = (*builtinInetAton)(nil)
	_ IR = (*builtinInetNtoa)(nil)
	_ IR = (*builtinInet6Aton)(nil)
	_ IR = (*builtinInet6Ntoa)(nil)
	_ IR = (*builtinIsIPV4)(nil)
	_ IR = (*builtinIsIPV4Compat)(nil)
	_ IR = (*builtinIsIPV4Mapped)(nil)
	_ IR = (*builtinIsIPV6)(nil)
	_ IR = (*builtinBinToUUID)(nil)
	_ IR = (*builtinIsUUID)(nil)
	_ IR = (*builtinUUID)(nil)
	_ IR = (*builtinUUIDToBin)(nil)
	_ IR = (*builtinLastInsertID)(nil)
)

func (call *builtinInetAton) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if arg == nil || err != nil {
		return nil, err
	}
	rawIp := evalToBinary(arg)
	ip, ok := mysqlInetAton(rawIp.string())
	if !ok {
		return nil, nil
	}
	return newEvalUint64(uint64(ip)), nil
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
		c.asm.Convert_xb(1, sqltypes.VarBinary, nil)
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
	return newEvalText(hack.StringBytes(netip.AddrFrom4([4]byte(b)).String()), typedCoercionCollation(sqltypes.VarChar, call.collate)), nil
}

func (call *builtinInetNtoa) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)

	c.compileToUint64(arg, 1)
	col := typedCoercionCollation(sqltypes.VarChar, call.collate)
	c.asm.Fn_INET_NTOA(col)
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.VarChar, Flag: flagNullable, Col: col}, nil
}

func (call *builtinInet6Aton) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if arg == nil || err != nil {
		return nil, err
	}
	rawIp := evalToBinary(arg)
	ip, ok := mysqlParseIP(rawIp.string())
	if !ok {
		return nil, nil
	}
	b := ip.AsSlice()
	return newEvalBinary(b), nil
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
		c.asm.Convert_xb(1, sqltypes.VarBinary, nil)
	}

	c.asm.Fn_INET6_ATON()
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.VarBinary, Flag: flagNullable, Col: collationBinary}, nil
}

func (call *builtinLastInsertID) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if err != nil {
		return nil, err
	}
	if arg == nil {
		env.VCursor().SetLastInsertID(0)
		return nil, err
	}
	insertID := uint64(evalToInt64(arg).i)
	env.VCursor().SetLastInsertID(insertID)
	return newEvalUint64(insertID), nil
}

func (call *builtinLastInsertID) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}
	c.asm.Fn_LAST_INSERT_ID()
	return ctype{Type: sqltypes.Uint64, Flag: arg.Flag & flagNullable, Col: collationNumeric}, nil
}

func (call *builtinLastInsertID) constant() bool {
	return false // we don't want this function to be simplified away
}

func printIPv6AsIPv4(addr netip.Addr) (netip.Addr, bool) {
	b := addr.AsSlice()
	if len(b) != 16 {
		return addr, false
	}
	for i := range 12 {
		if b[i] != 0 {
			return addr, false
		}
	}
	if b[12] == 0 && b[13] == 0 {
		return addr, false
	}
	return netip.AddrFrom4(([4]byte)(b[12:])), true
}

// mysqlInetAton parses an IPv4 address the way MySQL's INET_ATON() does.
// Unlike netip.ParseAddr, it accepts octets with leading zeros, which are
// read as decimal ("010.0.0.1" is 10.0.0.1), and the short forms "a", "a.b"
// and "a.b.c", where the last part is the last octet ("127.1" is 127.0.0.1).
func mysqlInetAton(s string) (uint32, bool) {
	var result uint64
	var octet uint64
	dots := 0
	last := byte('.') // an empty string is not an address
	for i := 0; i < len(s); i++ {
		last = s[i]
		switch {
		case last >= '0' && last <= '9':
			octet = octet*10 + uint64(last-'0')
			if octet > 255 {
				return 0, false
			}
		case last == '.':
			dots++
			if dots > 3 {
				return 0, false
			}
			result = result<<8 + octet
			octet = 0
		default:
			return 0, false
		}
	}
	if last == '.' {
		return 0, false
	}
	switch dots {
	case 1:
		result <<= 16
	case 2:
		result <<= 8
	}
	return uint32(result<<8 + octet), true
}

// mysqlParseIPv4 parses a dotted-quad IPv4 address the way MySQL's IS_IPV4()
// and INET6_ATON() do: exactly four decimal octets of one to three digits
// each, so octets with leading zeros ("010") are accepted.
func mysqlParseIPv4(s string) (netip.Addr, bool) {
	if len(s) < len("0.0.0.0") || len(s) > len("255.255.255.255") {
		return netip.Addr{}, false
	}
	var ip [4]byte
	octet, digits, dots := 0, 0, 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			digits++
			octet = octet*10 + int(c-'0')
			if digits > 3 || octet > 255 {
				return netip.Addr{}, false
			}
		case c == '.':
			if digits == 0 || dots == 3 {
				return netip.Addr{}, false
			}
			ip[dots] = byte(octet)
			dots++
			octet, digits = 0, 0
		default:
			return netip.Addr{}, false
		}
	}
	if digits == 0 || dots != 3 {
		return netip.Addr{}, false
	}
	ip[3] = byte(octet)
	return netip.AddrFrom4(ip), true
}

// mysqlParseIPv6 parses an IPv6 address the way MySQL's IS_IPV6() and
// INET6_ATON() do. Unlike netip.ParseAddr, it rejects zone identifiers
// ("fe80::1%eth0") and accepts an embedded IPv4 address whose octets have
// leading zeros ("::ffff:010.0.0.1").
func mysqlParseIPv6(s string) (netip.Addr, bool) {
	if len(s) < len("::") || len(s) > len("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff") {
		return netip.Addr{}, false
	}
	var ip [16]byte
	n := 0    // bytes written to ip
	gap := -1 // position of the "::" gap in ip, if any
	i := 0
	if s[0] == ':' {
		if s[1] != ':' {
			return netip.Addr{}, false
		}
		i = 1
	}
	groupStart := i
	group, digits := 0, 0
	for ; i < len(s); i++ {
		c := s[i]
		switch c {
		case ':':
			groupStart = i + 1
			if digits == 0 {
				if gap >= 0 {
					return netip.Addr{}, false
				}
				gap = n
				continue
			}
			if i+1 == len(s) || n+2 > len(ip) {
				return netip.Addr{}, false
			}
			ip[n], ip[n+1] = byte(group>>8), byte(group)
			n += 2
			group, digits = 0, 0
		case '.':
			if n+4 > len(ip) {
				return netip.Addr{}, false
			}
			ip4, ok := mysqlParseIPv4(s[groupStart:])
			if !ok {
				return netip.Addr{}, false
			}
			b := ip4.As4()
			copy(ip[n:], b[:])
			n += 4
			digits = 0
			i = len(s)
		default:
			var v byte
			switch {
			case c >= '0' && c <= '9':
				v = c - '0'
			case c >= 'a' && c <= 'f':
				v = c - 'a' + 10
			case c >= 'A' && c <= 'F':
				v = c - 'A' + 10
			default:
				return netip.Addr{}, false
			}
			if digits == 4 {
				return netip.Addr{}, false
			}
			group = group<<4 | int(v)
			digits++
		}
	}
	if digits > 0 {
		if n+2 > len(ip) {
			return netip.Addr{}, false
		}
		ip[n], ip[n+1] = byte(group>>8), byte(group)
		n += 2
	}
	if gap >= 0 {
		if n == len(ip) {
			return netip.Addr{}, false
		}
		tail := n - gap
		copy(ip[len(ip)-tail:], ip[gap:n])
		clear(ip[gap : len(ip)-tail])
	} else if n < len(ip) {
		return netip.Addr{}, false
	}
	return netip.AddrFrom16(ip), true
}

// mysqlParseIP parses an address the way MySQL's INET6_ATON() does: as an
// IPv4 address first, then as an IPv6 address.
func mysqlParseIP(s string) (netip.Addr, bool) {
	if ip, ok := mysqlParseIPv4(s); ok {
		return ip, true
	}
	return mysqlParseIPv6(s)
}

func isIPv4Compat(addr netip.Addr) bool {
	b := addr.AsSlice()
	if len(b) != 16 {
		return false
	}
	for i := range 12 {
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

	col := typedCoercionCollation(sqltypes.VarChar, call.collate)
	if ip, ok := printIPv6AsIPv4(ip); ok {
		return newEvalText(hack.StringBytes("::"+ip.String()), col), nil
	}

	return newEvalText(hack.StringBytes(ip.String()), col), nil
}

func (call *builtinInet6Ntoa) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}

	skip := c.compileNullCheck1(arg)
	col := typedCoercionCollation(sqltypes.VarChar, call.collate)

	switch arg.Type {
	case sqltypes.VarBinary, sqltypes.Blob, sqltypes.Binary:
		c.asm.Fn_INET6_NTOA(col)
	default:
		c.asm.SetNull(1)
	}
	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.VarChar, Flag: flagNullable, Col: col}, nil
}

func (call *builtinIsIPV4) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if arg == nil || err != nil {
		return nil, err
	}
	rawIp := evalToBinary(arg)
	_, ok := mysqlParseIPv4(rawIp.string())
	return newEvalBool(ok), nil
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
		c.asm.Convert_xb(1, sqltypes.VarBinary, nil)
	}

	c.asm.Fn_IS_IPV4()
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.Int64, Flag: nullableFlags(arg.Flag) | flagIsBoolean, Col: collationNumeric}, nil
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
	return ctype{Type: sqltypes.Int64, Flag: nullableFlags(arg.Flag) | flagIsBoolean, Col: collationNumeric}, nil
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
	return ctype{Type: sqltypes.Int64, Flag: nullableFlags(arg.Flag) | flagIsBoolean, Col: collationNumeric}, nil
}

func (call *builtinIsIPV6) eval(env *ExpressionEnv) (eval, error) {
	arg, err := call.arg1(env)
	if arg == nil || err != nil {
		return nil, err
	}
	rawIp := evalToBinary(arg)
	_, ok := mysqlParseIPv6(rawIp.string())
	return newEvalBool(ok), nil
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
		c.asm.Convert_xb(1, sqltypes.VarBinary, nil)
	}

	c.asm.Fn_IS_IPV6()
	c.asm.jumpDestination(skip)

	return ctype{Type: sqltypes.Int64, Flag: nullableFlags(arg.Flag) | flagIsBoolean, Col: collationNumeric}, nil
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
	return newEvalText(hack.StringBytes(parsed.String()), typedCoercionCollation(sqltypes.VarChar, call.collate)), nil
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
		c.asm.Convert_xb(1, sqltypes.VarBinary, nil)
	}

	col := typedCoercionCollation(sqltypes.VarChar, call.collate)
	ct := ctype{Type: sqltypes.VarChar, Flag: nullableFlags(arg.Flag), Col: col}

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

func (call *builtinIsUUID) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip := c.compileNullCheck1(arg)

	switch {
	case arg.isTextual():
	default:
		c.asm.Convert_xb(1, sqltypes.VarBinary, nil)
	}
	c.asm.Fn_IS_UUID()

	c.asm.jumpDestination(skip)
	return ctype{Type: sqltypes.Int64, Flag: nullableFlags(arg.Flag) | flagIsBoolean, Col: collationNumeric}, nil
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

func (call *builtinUUID) compile(c *compiler) (ctype, error) {
	c.asm.Fn_UUID()
	return ctype{Type: sqltypes.VarChar, Flag: 0, Col: collationUtf8mb3}, nil
}

func (call *builtinUUID) constant() bool {
	return false
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

func (call *builtinUUIDToBin) compile(c *compiler) (ctype, error) {
	arg, err := call.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}
	skip := c.compileNullCheck1(arg)

	switch {
	case arg.isTextual():
	default:
		c.asm.Convert_xb(1, sqltypes.VarBinary, nil)
	}

	ct := ctype{Type: sqltypes.VarBinary, Flag: nullableFlags(arg.Flag), Col: collationBinary}

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
