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
	"errors"
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/icuregex"
	icuerrors "vitess.io/vitess/go/mysql/icuregex/errors"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func regexpFlags(m eval, flags icuregex.RegexpFlag, f string) (icuregex.RegexpFlag, error) {
	switch m := m.(type) {
	case *evalBytes:
		for _, b := range m.bytes {
			switch b {
			case 'c':
				flags &= ^icuregex.CaseInsensitive
			case 'i':
				flags |= icuregex.CaseInsensitive
			case 'm':
				flags |= icuregex.Multiline
			case 'n':
				flags |= icuregex.DotAll
			case 'u':
				flags |= icuregex.UnixLines
			default:
				return flags, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongArguments, "Incorrect arguments to %s.", f)
			}
		}
	default:
		return flags, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongArguments, "Incorrect arguments to %s.", f)
	}

	return flags, nil
}

func occurrence(e *evalInt64, min int64) int64 {
	if e.i < min {
		return min
	}
	return e.i
}

func returnOption(val *evalInt64, f string) (int64, error) {
	switch val.i {
	case 0, 1:
		// Valid return options.
		return val.i, nil
	}
	return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongArguments, "Incorrect arguments to %s: return_option must be 1 or 0.", f)
}

func positionInstr(val *evalInt64, limit int64) (int64, error) {
	pos := val.i
	if pos < 1 || pos > limit {
		return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpIndexOutOfBounds, "Index out of bounds in regular expression search.")
	}
	return pos, nil
}

func position(val *evalInt64, limit int64, f string) (int64, error) {
	pos := val.i
	if pos < 1 {
		return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongParametersToNativeFct, "Incorrect parameters in the call to native function '%s'", f)
	}
	if pos-1 > limit {
		return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpIndexOutOfBounds, "Index out of bounds in regular expression search.")
	}
	return pos, nil
}

func evalRegexpCollation(input, pat eval, f string) (eval, eval, collations.TypedCollation, icuregex.RegexpFlag, error) {
	var typedCol collations.TypedCollation
	var err error

	if inputBytes, ok := input.(*evalBytes); ok {
		if patBytes, ok := pat.(*evalBytes); ok {
			inputCol := inputBytes.col.Collation
			patCol := patBytes.col.Collation
			if (inputCol == collations.CollationBinaryID && patCol != collations.CollationBinaryID) ||
				(inputCol != collations.CollationBinaryID && patCol == collations.CollationBinaryID) {
				inputColName := inputCol.Get().Name()
				patColName := patCol.Get().Name()
				return nil, nil, typedCol, 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.CharacterSetMismatch, "Character set '%s' cannot be used in conjunction with '%s' in call to %s.", inputColName, patColName, f)
			}
		}
	}

	input, pat, typedCol, err = mergeAndCoerceCollations(input, pat)
	if err != nil {
		return nil, nil, collations.TypedCollation{}, 0, err
	}

	var flags icuregex.RegexpFlag
	var collation = typedCol.Collation.Get()
	if strings.Contains(collation.Name(), "_ci") {
		flags |= icuregex.CaseInsensitive
	}

	return input, pat, typedCol, flags, nil
}

func compileRegexpCollation(input, pat ctype, f string) (collations.TypedCollation, icuregex.RegexpFlag, error) {
	var merged collations.TypedCollation
	var err error

	if input.isTextual() && pat.isTextual() {
		inputCol := input.Col.Collation
		patCol := pat.Col.Collation
		if (inputCol == collations.CollationBinaryID && patCol != collations.CollationBinaryID) ||
			(inputCol != collations.CollationBinaryID && patCol == collations.CollationBinaryID) {
			inputColName := inputCol.Get().Name()
			patColName := patCol.Get().Name()
			return input.Col, 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.CharacterSetMismatch, "Character set '%s' cannot be used in conjunction with '%s' in call to %s.", inputColName, patColName, f)
		}
	}

	if input.Col.Collation != pat.Col.Collation {
		merged, _, _, err = mergeCollations(input.Col, pat.Col, input.Type, pat.Type)
	} else {
		merged = input.Col
	}
	if err != nil {
		return input.Col, 0, err
	}

	var flags icuregex.RegexpFlag
	var collation = merged.Collation.Get()
	if strings.Contains(collation.Name(), "_ci") {
		flags |= icuregex.CaseInsensitive
	}
	return merged, flags, nil
}

func compileRegex(pat eval, c collations.Charset, flags icuregex.RegexpFlag) (*icuregex.Pattern, error) {
	patRunes := charset.Expand(nil, pat.ToRawBytes(), c)

	if len(patRunes) == 0 {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpIllegalArgument, "Illegal argument to a regular expression.")
	}

	regexp, err := icuregex.Compile(patRunes, flags)
	if err == nil {
		return regexp, nil
	}

	var compileErr *icuregex.CompileError
	if errors.Is(err, icuerrors.ErrUnsupported) {
		err = vterrors.NewErrorf(vtrpcpb.Code_UNIMPLEMENTED, vterrors.RegexpUnimplemented, err.Error())
	} else if errors.Is(err, icuerrors.ErrIllegalArgument) {
		err = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpIllegalArgument, err.Error())
	} else if errors.As(err, &compileErr) {
		switch compileErr.Code {
		case icuregex.InternalError:
			err = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpInternal, compileErr.Error())
		case icuregex.RuleSyntax:
			err = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpRuleSyntax, compileErr.Error())
		case icuregex.BadEscapeSequence:
			err = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpBadEscapeSequence, compileErr.Error())
		case icuregex.PropertySyntax:
			err = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpRuleSyntax, compileErr.Error())
		case icuregex.Unimplemented:
			err = vterrors.NewErrorf(vtrpcpb.Code_UNIMPLEMENTED, vterrors.RegexpUnimplemented, compileErr.Error())
		case icuregex.MismatchedParen:
			err = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpMismatchParen, compileErr.Error())
		case icuregex.BadInterval:
			err = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpBadInterval, compileErr.Error())
		case icuregex.MaxLtMin:
			err = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpMaxLtMin, compileErr.Error())
		case icuregex.InvalidBackRef:
			err = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpInvalidBackRef, compileErr.Error())
		case icuregex.InvalidFlag:
			err = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpInvalidFlag, compileErr.Error())
		case icuregex.LookBehindLimit:
			err = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpLookBehindLimit, compileErr.Error())
		case icuregex.MissingCloseBracket:
			err = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpMissingCloseBracket, compileErr.Error())
		case icuregex.InvalidRange:
			err = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpInvalidRange, compileErr.Error())
		case icuregex.PatternTooBig:
			err = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpPatternTooBig, compileErr.Error())
		case icuregex.InvalidCaptureGroupName:
			err = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpInvalidCaptureGroup, compileErr.Error())
		default:
			err = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpInternal, compileErr.Error())
		}
	}

	return nil, err
}

func compileConstantRegex(c *compiler, args TupleExpr, pat, mt int, cs collations.TypedCollation, flags icuregex.RegexpFlag, f string) (*icuregex.Pattern, error) {
	pattern := args[pat]
	if !pattern.constant() {
		return nil, c.unsupported(pattern)
	}
	var err error
	staticEnv := EmptyExpressionEnv()
	pattern, err = simplifyExpr(staticEnv, pattern)
	if err != nil {
		return nil, err
	}

	if len(args) > mt {
		fl := args[mt]
		if !fl.constant() {
			return nil, c.unsupported(fl)
		}
		fl, err = simplifyExpr(staticEnv, fl)
		if err != nil {
			return nil, err
		}
		flags, err = regexpFlags(fl.(*Literal).inner, flags, f)
		if err != nil {
			return nil, err
		}
	}

	if pattern.(*Literal).inner == nil {
		return nil, c.unsupported(pattern)
	}

	innerPat, err := evalToVarchar(pattern.(*Literal).inner, cs.Collation, true)
	if err != nil {
		return nil, err
	}

	return compileRegex(innerPat, cs.Collation.Get().Charset(), flags)
}

// resultCollation returns the collation to use for the result of a regexp.
// This falls back to latin1_swedish if the input collation is binary. This
// seems to be a side effect of how MySQL also works. Probably due to how it
// is using ICU and converting there.
func resultCollation(in collations.TypedCollation) collations.TypedCollation {
	if in.Collation == collationBinary.Collation {
		return collationRegexpFallback
	}
	return in
}

type builtinRegexpLike struct {
	CallExpr
	Negate bool
}

func (r *builtinRegexpLike) eval(env *ExpressionEnv) (eval, error) {
	input, err := r.Arguments[0].eval(env)
	if err != nil || input == nil {
		return nil, err
	}

	pat, err := r.Arguments[1].eval(env)
	if err != nil || pat == nil {
		return nil, err
	}

	input, pat, typedCol, flags, err := evalRegexpCollation(input, pat, "regexp_like")
	if err != nil {
		return nil, err
	}
	collation := typedCol.Collation.Get()

	if len(r.Arguments) > 2 {
		m, err := r.Arguments[2].eval(env)
		if err != nil || m == nil {
			return nil, err
		}
		flags, err = regexpFlags(m, flags, "regexp_like")
		if err != nil {
			return nil, err
		}
	}

	regexp, err := compileRegex(pat, collation.Charset(), flags)
	if err != nil {
		return nil, err
	}

	inputRunes := charset.Expand(nil, input.ToRawBytes(), collation.Charset())
	m := icuregex.NewMatcher(regexp)
	m.Reset(inputRunes)

	ok, err := m.Find()
	if err != nil {
		return nil, err
	}
	if r.Negate {
		ok = !ok
	}
	return newEvalBool(ok), nil
}

func (r *builtinRegexpLike) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f1 := r.Arguments[0].typeof(env, fields)
	_, f2 := r.Arguments[1].typeof(env, fields)
	var f3 typeFlag
	if len(r.Arguments) > 2 {
		_, f3 = r.Arguments[2].typeof(env, fields)
	}
	return sqltypes.Int64, f1 | f2 | f3 | flagIsBoolean
}

func (r *builtinRegexpLike) compileSlow(c *compiler, input, pat, fl ctype, merged collations.TypedCollation, flags icuregex.RegexpFlag, skips ...*jump) (ctype, error) {
	if !pat.isTextual() || pat.Col.Collation != merged.Collation {
		c.asm.Convert_xce(len(r.Arguments)-1, sqltypes.VarChar, merged.Collation)
	}

	c.asm.Fn_REGEXP_LIKE_slow(r.Negate, merged.Collation.Get().Charset(), flags, len(r.Arguments)-1)
	c.asm.jumpDestination(skips...)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: input.Flag | pat.Flag | fl.Flag | flagIsBoolean}, nil
}

func (r *builtinRegexpLike) compile(c *compiler) (ctype, error) {
	input, err := r.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}
	var skips []*jump
	skips = append(skips, c.compileNullCheckArg(input, 0))

	pat, err := r.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}
	skips = append(skips, c.compileNullCheckArg(pat, 1))

	var f ctype

	if len(r.Arguments) > 2 {
		f, err = r.Arguments[2].compile(c)
		if err != nil {
			return ctype{}, err
		}
		skips = append(skips, c.compileNullCheckArg(f, 2))
	}

	merged, flags, err := compileRegexpCollation(input, pat, "regexp_like")
	if err != nil {
		return ctype{}, err
	}

	if !input.isTextual() || input.Col.Collation != merged.Collation {
		c.asm.Convert_xce(len(r.Arguments), sqltypes.VarChar, merged.Collation)
	}

	// We optimize for the case where the pattern is a constant. If not,
	// we fall back to the slow path.
	p, err := compileConstantRegex(c, r.Arguments, 1, 2, merged, flags, "regexp_like")
	if err != nil {
		return r.compileSlow(c, input, pat, f, merged, flags, skips...)
	}

	c.asm.Fn_REGEXP_LIKE(icuregex.NewMatcher(p), r.Negate, merged.Collation.Get().Charset(), len(r.Arguments)-1)
	c.asm.jumpDestination(skips...)

	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: input.Flag | pat.Flag | f.Flag | flagIsBoolean}, nil
}

var _ Expr = (*builtinRegexpLike)(nil)

type builtinRegexpInstr struct {
	CallExpr
}

func (r *builtinRegexpInstr) eval(env *ExpressionEnv) (eval, error) {
	input, err := r.Arguments[0].eval(env)
	if err != nil || input == nil {
		return nil, err
	}

	pat, err := r.Arguments[1].eval(env)
	if err != nil || pat == nil {
		return nil, err
	}

	input, pat, typedCol, flags, err := evalRegexpCollation(input, pat, "regexp_instr")
	if err != nil {
		return nil, err
	}

	var posExpr eval
	if len(r.Arguments) > 2 {
		posExpr, err = r.Arguments[2].eval(env)
		if err != nil || posExpr == nil {
			return nil, err
		}
	}

	var occExpr eval
	if len(r.Arguments) > 3 {
		occExpr, err = r.Arguments[3].eval(env)
		if err != nil || occExpr == nil {
			return nil, err
		}
	}

	var retExpr eval
	if len(r.Arguments) > 4 {
		retExpr, err = r.Arguments[4].eval(env)
		if err != nil || retExpr == nil {
			return nil, err
		}
	}

	var mtExpr eval
	if len(r.Arguments) > 5 {
		mtExpr, err = r.Arguments[5].eval(env)
		if err != nil || mtExpr == nil {
			return nil, err
		}
	}

	collation := typedCol.Collation.Get()

	pos := int64(1)
	occ := int64(1)
	returnOpt := int64(0)

	if mtExpr != nil {
		flags, err = regexpFlags(mtExpr, flags, "regexp_instr")
		if err != nil {
			return nil, err
		}
	}

	regexp, err := compileRegex(pat, collation.Charset(), flags)
	if err != nil {
		return nil, err
	}

	inputRunes := charset.Expand(nil, input.ToRawBytes(), collation.Charset())
	if len(inputRunes) == 0 {
		return newEvalInt64(0), nil
	}

	if posExpr != nil {
		pos, err = positionInstr(evalToInt64(posExpr), int64(len(inputRunes)))
		if err != nil {
			return nil, err
		}
	}

	if occExpr != nil {
		occ = occurrence(evalToInt64(occExpr), occ)
	}

	if retExpr != nil {
		returnOpt, err = returnOption(evalToInt64(retExpr), "regexp_instr")
		if err != nil {
			return nil, err
		}
	}

	m := icuregex.NewMatcher(regexp)
	m.Reset(inputRunes[pos-1:])

	found := false
	for i := int64(0); i < occ; i++ {
		found, err = m.Find()
		if err != nil {
			return nil, err
		}
		if !found {
			break
		}
	}
	if !found {
		return newEvalInt64(0), nil
	}
	if returnOpt == 0 {
		return newEvalInt64(int64(m.Start()) + pos), nil
	}
	return newEvalInt64(int64(m.End()) + pos), nil
}

func (r *builtinRegexpInstr) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f1 := r.Arguments[0].typeof(env, fields)
	_, f2 := r.Arguments[1].typeof(env, fields)
	var f3, f4, f5, f6 typeFlag
	if len(r.Arguments) > 2 {
		_, f3 = r.Arguments[2].typeof(env, fields)
	}
	if len(r.Arguments) > 3 {
		_, f4 = r.Arguments[3].typeof(env, fields)
	}
	if len(r.Arguments) > 4 {
		_, f5 = r.Arguments[4].typeof(env, fields)
	}
	if len(r.Arguments) > 5 {
		_, f6 = r.Arguments[5].typeof(env, fields)
	}
	return sqltypes.Int64, f1 | f2 | f3 | f4 | f5 | f6
}

func (r *builtinRegexpInstr) compileSlow(c *compiler, input, pat, pos, occ, returnOption, matchType ctype, merged collations.TypedCollation, flags icuregex.RegexpFlag, skips ...*jump) (ctype, error) {
	if !pat.isTextual() || pat.Col.Collation != merged.Collation {
		c.asm.Convert_xce(len(r.Arguments)-1, sqltypes.VarChar, merged.Collation)
	}

	c.asm.Fn_REGEXP_INSTR_slow(merged.Collation.Get().Charset(), flags, len(r.Arguments)-1)
	c.asm.jumpDestination(skips...)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: input.Flag | pat.Flag | pos.Flag | occ.Flag | returnOption.Flag | matchType.Flag}, nil
}

func (r *builtinRegexpInstr) compile(c *compiler) (ctype, error) {
	input, err := r.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}
	var skips []*jump
	skips = append(skips, c.compileNullCheckArg(input, 0))

	pat, err := r.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}
	skips = append(skips, c.compileNullCheckArg(pat, 1))

	var pos ctype
	if len(r.Arguments) > 2 {
		pos, err = r.Arguments[2].compile(c)
		if err != nil {
			return ctype{}, err
		}
		skips = append(skips, c.compileNullCheckArg(pos, 2))
		_ = c.compileToInt64(pos, 1)
	}

	var occ ctype
	if len(r.Arguments) > 3 {
		occ, err = r.Arguments[3].compile(c)
		if err != nil {
			return ctype{}, err
		}
		skips = append(skips, c.compileNullCheckArg(occ, 3))
		_ = c.compileToInt64(occ, 1)
	}

	var returnOpt ctype
	if len(r.Arguments) > 4 {
		returnOpt, err = r.Arguments[4].compile(c)
		if err != nil {
			return ctype{}, err
		}
		skips = append(skips, c.compileNullCheckArg(returnOpt, 4))
		_ = c.compileToInt64(returnOpt, 1)
	}

	var matchType ctype
	if len(r.Arguments) > 5 {
		matchType, err = r.Arguments[5].compile(c)
		if err != nil {
			return ctype{}, err
		}
		skips = append(skips, c.compileNullCheckArg(matchType, 5))
		switch {
		case matchType.isTextual():
		default:
			c.asm.Convert_xb(1, sqltypes.VarBinary, 0, false)
		}
	}

	merged, flags, err := compileRegexpCollation(input, pat, "regexp_instr")
	if err != nil {
		return ctype{}, err
	}

	if !input.isTextual() || input.Col.Collation != merged.Collation {
		c.asm.Convert_xce(len(r.Arguments), sqltypes.VarChar, merged.Collation)
	}

	// We optimize for the case where the pattern is a constant. If not,
	// we fall back to the slow path.
	p, err := compileConstantRegex(c, r.Arguments, 1, 5, merged, flags, "regexp_instr")
	if err != nil {
		return r.compileSlow(c, input, pat, pos, occ, returnOpt, matchType, merged, flags, skips...)
	}

	c.asm.Fn_REGEXP_INSTR(icuregex.NewMatcher(p), merged.Collation.Get().Charset(), len(r.Arguments)-1)
	c.asm.jumpDestination(skips...)

	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: input.Flag | pat.Flag | flagIsBoolean}, nil
}

var _ Expr = (*builtinRegexpInstr)(nil)

type builtinRegexpSubstr struct {
	CallExpr
}

func (r *builtinRegexpSubstr) eval(env *ExpressionEnv) (eval, error) {
	input, err := r.Arguments[0].eval(env)
	if err != nil || input == nil {
		return nil, err
	}

	pat, err := r.Arguments[1].eval(env)
	if err != nil || pat == nil {
		return nil, err
	}

	input, pat, typedCol, flags, err := evalRegexpCollation(input, pat, "regexp_substr")
	if err != nil {
		return nil, err
	}

	var posExpr eval
	// For some reason this gets checked before NULL checks of the other values
	if len(r.Arguments) > 2 {
		posExpr, err = r.Arguments[2].eval(env)
		if err != nil || posExpr == nil {
			return nil, err
		}
	}

	var occExpr eval
	if len(r.Arguments) > 3 {
		occExpr, err = r.Arguments[3].eval(env)
		if err != nil || occExpr == nil {
			return nil, err
		}
	}

	var mtExpr eval
	if len(r.Arguments) > 4 {
		mtExpr, err = r.Arguments[4].eval(env)
		if err != nil || mtExpr == nil {
			return nil, err
		}
	}

	collation := typedCol.Collation.Get()
	pos := int64(1)
	occ := int64(1)
	inputRunes := charset.Expand(nil, input.ToRawBytes(), collation.Charset())

	if posExpr != nil {
		pos, err = position(evalToInt64(posExpr), int64(len(inputRunes)), "regexp_substr")
		if err != nil {
			return nil, err
		}

	}

	if occExpr != nil {
		occ = occurrence(evalToInt64(occExpr), occ)
	}

	if mtExpr != nil {
		flags, err = regexpFlags(mtExpr, flags, "regexp_substr")
		if err != nil {
			return nil, err
		}
	}

	regexp, err := compileRegex(pat, collation.Charset(), flags)
	if err != nil {
		return nil, err
	}

	m := icuregex.NewMatcher(regexp)
	m.Reset(inputRunes[pos-1:])

	found := false
	for i := int64(0); i < occ; i++ {
		found, err = m.Find()
		if err != nil {
			return nil, err
		}
		if !found {
			break
		}
	}
	if !found {
		return nil, nil
	}
	out := inputRunes[int64(m.Start())+pos-1 : int64(m.End())+pos-1]
	b := charset.Collapse(nil, out, collation.Charset())
	return newEvalText(b, resultCollation(typedCol)), nil
}

func (r *builtinRegexpSubstr) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f1 := r.Arguments[0].typeof(env, fields)
	_, f2 := r.Arguments[1].typeof(env, fields)
	var f3, f4, f5 typeFlag
	if len(r.Arguments) > 2 {
		_, f3 = r.Arguments[2].typeof(env, fields)
	}
	if len(r.Arguments) > 3 {
		_, f4 = r.Arguments[3].typeof(env, fields)
	}
	if len(r.Arguments) > 4 {
		_, f5 = r.Arguments[4].typeof(env, fields)
	}
	return sqltypes.VarChar, f1 | f2 | f3 | f4 | f5
}

func (r *builtinRegexpSubstr) compileSlow(c *compiler, input, pat, pos, occ, matchType ctype, merged collations.TypedCollation, flags icuregex.RegexpFlag, skips ...*jump) (ctype, error) {
	if !pat.isTextual() || pat.Col.Collation != merged.Collation {
		c.asm.Convert_xce(len(r.Arguments)-1, sqltypes.VarChar, merged.Collation)
	}

	c.asm.Fn_REGEXP_SUBSTR_slow(merged, flags, len(r.Arguments)-1)
	c.asm.jumpDestination(skips...)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: input.Flag | pat.Flag | pos.Flag | occ.Flag | matchType.Flag}, nil
}

func (r *builtinRegexpSubstr) compile(c *compiler) (ctype, error) {
	input, err := r.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}
	var skips []*jump
	skips = append(skips, c.compileNullCheckArg(input, 0))

	pat, err := r.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}
	skips = append(skips, c.compileNullCheckArg(pat, 1))

	var pos ctype
	if len(r.Arguments) > 2 {
		pos, err = r.Arguments[2].compile(c)
		if err != nil {
			return ctype{}, err
		}
		skips = append(skips, c.compileNullCheckArg(pos, 2))
		_ = c.compileToInt64(pos, 1)
	}

	var occ ctype
	if len(r.Arguments) > 3 {
		occ, err = r.Arguments[3].compile(c)
		if err != nil {
			return ctype{}, err
		}
		skips = append(skips, c.compileNullCheckArg(occ, 3))
		_ = c.compileToInt64(occ, 1)
	}

	var matchType ctype
	if len(r.Arguments) > 4 {
		matchType, err = r.Arguments[4].compile(c)
		if err != nil {
			return ctype{}, err
		}
		skips = append(skips, c.compileNullCheckArg(matchType, 4))
		switch {
		case matchType.isTextual():
		default:
			c.asm.Convert_xb(1, sqltypes.VarBinary, 0, false)
		}
	}

	merged, flags, err := compileRegexpCollation(input, pat, "regexp_substr")
	if err != nil {
		return ctype{}, err
	}

	if !input.isTextual() || input.Col.Collation != merged.Collation {
		c.asm.Convert_xce(len(r.Arguments), sqltypes.VarChar, merged.Collation)
	}

	// We optimize for the case where the pattern is a constant. If not,
	// we fall back to the slow path.
	p, err := compileConstantRegex(c, r.Arguments, 1, 4, merged, flags, "regexp_substr")
	if err != nil {
		return r.compileSlow(c, input, pat, pos, occ, matchType, merged, flags, skips...)
	}

	c.asm.Fn_REGEXP_SUBSTR(icuregex.NewMatcher(p), merged, len(r.Arguments)-1)
	c.asm.jumpDestination(skips...)

	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: input.Flag | pat.Flag | pos.Flag | occ.Flag | matchType.Flag}, nil
}

var _ Expr = (*builtinRegexpSubstr)(nil)

type builtinRegexpReplace struct {
	CallExpr
}

func regexpReplace(m *icuregex.Matcher, inputRunes, replRunes []rune, pos, occ int64, c collations.Charset) ([]byte, bool, error) {
	var err error
	found := false
	if occ > 0 {
		for i := int64(0); i < occ; i++ {
			found, err = m.Find()
			if err != nil {
				return nil, false, err
			}
			if !found {
				break
			}
		}
		if !found {
			return nil, false, nil
		}

		out := append(inputRunes[:int64(m.Start())+pos-1], replRunes...)
		out = append(out, inputRunes[int64(m.End())+pos-1:]...)
		return charset.Collapse(nil, out, c), true, nil
	}

	found, err = m.Find()
	if err != nil {
		return nil, false, err
	}

	if !found {
		return nil, false, nil
	}

	start := int64(m.Start()) + pos - 1
	out := append(inputRunes[:start], replRunes...)
	end := int64(m.End()) + pos - 1
	for {
		found, err = m.Find()
		if err != nil {
			return nil, false, err
		}
		if !found {
			break
		}
		nextStart := int64(m.Start()) + pos - 1
		out = append(out, inputRunes[end:nextStart]...)
		out = append(out, replRunes...)
		end = int64(m.End()) + pos - 1
	}

	out = append(out, inputRunes[end:]...)
	return charset.Collapse(nil, out, c), true, nil
}

func (r *builtinRegexpReplace) eval(env *ExpressionEnv) (eval, error) {
	input, err := r.Arguments[0].eval(env)
	if err != nil || input == nil {
		return nil, err
	}

	pat, err := r.Arguments[1].eval(env)
	if err != nil || pat == nil {
		return nil, err
	}

	replArg, err := r.Arguments[2].eval(env)
	if err != nil || replArg == nil {
		return nil, err
	}

	input, pat, typedCol, flags, err := evalRegexpCollation(input, pat, "regexp_replace")
	if err != nil {
		return nil, err
	}

	var posExpr eval
	// For some reason this gets checked before NULL checks of the other values
	if len(r.Arguments) > 3 {
		posExpr, err = r.Arguments[3].eval(env)
		if err != nil || posExpr == nil {
			return nil, err
		}
	}

	var occExpr eval
	if len(r.Arguments) > 4 {
		occExpr, err = r.Arguments[4].eval(env)
		if err != nil || occExpr == nil {
			return nil, err
		}
	}

	var mtExpr eval
	if len(r.Arguments) > 5 {
		mtExpr, err = r.Arguments[5].eval(env)
		if err != nil || mtExpr == nil {
			return nil, err
		}
	}

	collation := typedCol.Collation.Get()

	repl, ok := replArg.(*evalBytes)
	if !ok {
		repl, err = evalToVarchar(replArg, typedCol.Collation, true)
		if err != nil {
			return nil, err
		}
	}
	pos := int64(1)
	occ := int64(0)
	inputRunes := charset.Expand(nil, input.ToRawBytes(), collation.Charset())
	replRunes := charset.Expand(nil, repl.ToRawBytes(), repl.col.Collation.Get().Charset())

	if posExpr != nil {
		pos, err = position(evalToInt64(posExpr), int64(len(inputRunes)), "regexp_replace")
		if err != nil {
			return nil, err
		}
	}

	if occExpr != nil {
		occ = occurrence(evalToInt64(occExpr), occ)
	}

	if mtExpr != nil {
		flags, err = regexpFlags(mtExpr, flags, "regexp_replace")
		if err != nil {
			return nil, err
		}
	}

	regexp, err := compileRegex(pat, collation.Charset(), flags)
	if err != nil {
		return nil, err
	}

	m := icuregex.NewMatcher(regexp)
	m.Reset(inputRunes[pos-1:])

	bytes, replaced, err := regexpReplace(m, inputRunes, replRunes, pos, occ, collation.Charset())
	if err != nil {
		return nil, err
	}
	if !replaced {
		return newEvalRaw(sqltypes.Text, input.ToRawBytes(), resultCollation(typedCol)), nil
	}
	return newEvalRaw(sqltypes.Text, bytes, resultCollation(typedCol)), nil
}

func (r *builtinRegexpReplace) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f1 := r.Arguments[0].typeof(env, fields)
	_, f2 := r.Arguments[1].typeof(env, fields)
	_, f3 := r.Arguments[2].typeof(env, fields)
	var f4, f5, f6 typeFlag
	if len(r.Arguments) > 3 {
		_, f4 = r.Arguments[3].typeof(env, fields)
	}
	if len(r.Arguments) > 4 {
		_, f5 = r.Arguments[4].typeof(env, fields)
	}
	if len(r.Arguments) > 5 {
		_, f6 = r.Arguments[5].typeof(env, fields)
	}
	return sqltypes.Text, f1 | f2 | f3 | f4 | f5 | f6
}

func (r *builtinRegexpReplace) compileSlow(c *compiler, input, pat, repl, pos, occ, matchType ctype, merged collations.TypedCollation, flags icuregex.RegexpFlag, skips ...*jump) (ctype, error) {
	if !pat.isTextual() || pat.Col.Collation != merged.Collation {
		c.asm.Convert_xce(len(r.Arguments)-1, sqltypes.VarChar, merged.Collation)
	}

	c.asm.Fn_REGEXP_REPLACE_slow(merged, flags, len(r.Arguments)-1)
	c.asm.jumpDestination(skips...)
	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: input.Flag | pat.Flag | repl.Flag | pos.Flag | occ.Flag | matchType.Flag}, nil
}

func (r *builtinRegexpReplace) compile(c *compiler) (ctype, error) {
	input, err := r.Arguments[0].compile(c)
	if err != nil {
		return ctype{}, err
	}
	var skips []*jump
	skips = append(skips, c.compileNullCheckArg(input, 0))

	pat, err := r.Arguments[1].compile(c)
	if err != nil {
		return ctype{}, err
	}
	skips = append(skips, c.compileNullCheckArg(pat, 1))

	repl, err := r.Arguments[2].compile(c)
	if err != nil {
		return ctype{}, err
	}
	skips = append(skips, c.compileNullCheckArg(repl, 2))

	var pos ctype
	if len(r.Arguments) > 3 {
		pos, err = r.Arguments[3].compile(c)
		if err != nil {
			return ctype{}, err
		}
		skips = append(skips, c.compileNullCheckArg(pos, 3))
		_ = c.compileToInt64(pos, 1)
	}

	var occ ctype
	if len(r.Arguments) > 4 {
		occ, err = r.Arguments[4].compile(c)
		if err != nil {
			return ctype{}, err
		}
		skips = append(skips, c.compileNullCheckArg(occ, 4))
		_ = c.compileToInt64(occ, 1)
	}

	var matchType ctype
	if len(r.Arguments) > 5 {
		matchType, err = r.Arguments[5].compile(c)
		if err != nil {
			return ctype{}, err
		}
		skips = append(skips, c.compileNullCheckArg(matchType, 5))
		switch {
		case matchType.isTextual():
		default:
			c.asm.Convert_xb(1, sqltypes.VarBinary, 0, false)
		}
	}

	merged, flags, err := compileRegexpCollation(input, pat, "regexp_replace")
	if err != nil {
		return ctype{}, err
	}

	if !input.isTextual() || input.Col.Collation != merged.Collation {
		c.asm.Convert_xce(len(r.Arguments), sqltypes.VarChar, merged.Collation)
	}

	if !repl.isTextual() || repl.Col.Collation != merged.Collation {
		c.asm.Convert_xce(len(r.Arguments)-2, sqltypes.VarChar, merged.Collation)
	}

	// We optimize for the case where the pattern is a constant. If not,
	// we fall back to the slow path.
	p, err := compileConstantRegex(c, r.Arguments, 1, 5, merged, flags, "regexp_replace")
	if err != nil {
		return r.compileSlow(c, input, pat, repl, pos, occ, matchType, merged, flags, skips...)
	}

	c.asm.Fn_REGEXP_REPLACE(icuregex.NewMatcher(p), merged, len(r.Arguments)-1)
	c.asm.jumpDestination(skips...)

	return ctype{Type: sqltypes.Int64, Col: collationNumeric, Flag: input.Flag | pat.Flag | repl.Flag | pos.Flag | occ.Flag | matchType.Flag}, nil
}

var _ Expr = (*builtinRegexpReplace)(nil)
