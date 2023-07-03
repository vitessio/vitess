package evalengine

import (
	"errors"
	"strings"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/icuregex"
	icuerrors "vitess.io/vitess/go/mysql/icuregex/errors"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func evalRegexpFlags(env *ExpressionEnv, match Expr, flags icuregex.RegexpFlag) (icuregex.RegexpFlag, error) {
	m, err := match.eval(env)
	if err != nil || m == nil {
		return flags, err
	}

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
				return flags, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongArguments, "Incorrect arguments to regexp_instr.")
			}
		}
	default:
		return flags, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongArguments, "Incorrect arguments to regexp_instr.")
	}

	return flags, nil
}

func evalOccurrence(env *ExpressionEnv, expr Expr) (int64, error) {
	occExpr, err := expr.eval(env)
	if err != nil {
		return 0, err
	}
	return evalToInt64(occExpr).i, nil
}

func evalReturnOption(env *ExpressionEnv, expr Expr) (int64, error) {
	retExpr, err := expr.eval(env)
	if err != nil {
		return 0, err
	}
	returnOption := evalToInt64(retExpr).i
	switch returnOption {
	case 0, 1:
		// Valid return options.
		return returnOption, nil
	}
	return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongArguments, "Incorrect arguments to regexp_instr: return_option must be 1 or 0.")
}

func evalPosition(env *ExpressionEnv, expr Expr, limit int64) (int64, error) {
	posExpr, err := expr.eval(env)
	if err != nil {
		return 0, err
	}
	pos := evalToInt64(posExpr).i
	if pos < 1 || pos > limit {
		return 0, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.RegexpIndexOutOfBounds, "Index out of bounds in regular expression search.")
	}
	return pos, nil
}

func compileRegex(pat eval, c collations.Charset, flags icuregex.RegexpFlag) (*icuregex.Pattern, error) {
	patUtf8, err := charset.Convert(nil, &charset.Charset_utf8mb4{}, pat.ToRawBytes(), c)
	if err != nil {
		return nil, err
	}

	regexp, err := icuregex.CompileString(hack.String(patUtf8), flags)
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

	var typedCol collations.TypedCollation
	input, pat, typedCol, err = mergeAndCoerceCollations(input, pat)
	if err != nil {
		return nil, err
	}

	var flags icuregex.RegexpFlag
	var collation = typedCol.Collation.Get()
	if strings.Contains(collation.Name(), "_ci") {
		flags |= icuregex.CaseInsensitive
	}

	if len(r.Arguments) > 2 {
		flags, err = evalRegexpFlags(env, r.Arguments[2], flags)
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

	ok, err := m.Matches()
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
	return sqltypes.Int64, f1 | f2 | flagIsBoolean
}

func (r *builtinRegexpLike) compile(c *compiler) (ctype, error) {
	return ctype{}, c.unsupported(r)
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

	var typedCol collations.TypedCollation
	input, pat, typedCol, err = mergeAndCoerceCollations(input, pat)
	if err != nil {
		return nil, err
	}

	var flags icuregex.RegexpFlag
	var collation = typedCol.Collation.Get()
	if strings.Contains(collation.Name(), "_ci") {
		flags |= icuregex.CaseInsensitive
	}

	pos := int64(1)
	occurrence := int64(1)
	returnOption := int64(0)
	inputRunes := charset.Expand(nil, input.ToRawBytes(), collation.Charset())

	if len(r.Arguments) > 2 {
		pos, err = evalPosition(env, r.Arguments[2], int64(len(inputRunes)))
		if err != nil {
			return nil, err
		}
	}

	if len(r.Arguments) > 3 {
		occurrence, err = evalOccurrence(env, r.Arguments[3])
		if err != nil {
			return nil, err
		}
	}

	if len(r.Arguments) > 4 {
		returnOption, err = evalReturnOption(env, r.Arguments[4])
		if err != nil {
			return nil, err
		}
	}

	if len(r.Arguments) > 5 {
		flags, err = evalRegexpFlags(env, r.Arguments[5], flags)
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
	for i := int64(0); i < occurrence; i++ {
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
	if returnOption == 0 {
		return newEvalInt64(int64(m.Start()) + pos), nil
	}
	return newEvalInt64(int64(m.End()) + pos), nil
}

func (r *builtinRegexpInstr) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f1 := r.Arguments[0].typeof(env, fields)
	_, f2 := r.Arguments[1].typeof(env, fields)
	return sqltypes.Int64, f1 | f2
}

func (r *builtinRegexpInstr) compile(c *compiler) (ctype, error) {
	return ctype{}, c.unsupported(r)
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

	var typedCol collations.TypedCollation
	input, pat, typedCol, err = mergeAndCoerceCollations(input, pat)
	if err != nil {
		return nil, err
	}

	var flags icuregex.RegexpFlag
	var collation = typedCol.Collation.Get()
	if strings.Contains(collation.Name(), "_ci") {
		flags |= icuregex.CaseInsensitive
	}

	pos := int64(1)
	occurrence := int64(1)
	inputRunes := charset.Expand(nil, input.ToRawBytes(), collation.Charset())

	if len(r.Arguments) > 2 {
		pos, err = evalPosition(env, r.Arguments[2], int64(len(inputRunes)))
		if err != nil {
			return nil, err
		}
	}

	if len(r.Arguments) > 3 {
		occurrence, err = evalOccurrence(env, r.Arguments[3])
		if err != nil {
			return nil, err
		}
	}

	if len(r.Arguments) > 4 {
		flags, err = evalRegexpFlags(env, r.Arguments[4], flags)
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
	for i := int64(0); i < occurrence; i++ {
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
	bytes := charset.Collapse(nil, out, collation.Charset())
	return newEvalText(bytes, typedCol), nil
}

func (r *builtinRegexpSubstr) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f1 := r.Arguments[0].typeof(env, fields)
	_, f2 := r.Arguments[1].typeof(env, fields)
	return sqltypes.VarChar, f1 | f2
}

func (r *builtinRegexpSubstr) compile(c *compiler) (ctype, error) {
	return ctype{}, c.unsupported(r)
}

var _ Expr = (*builtinRegexpSubstr)(nil)

type builtinRegexpReplace struct {
	CallExpr
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
	if err != nil || pat == nil {
		return nil, err
	}

	var typedCol collations.TypedCollation
	input, pat, typedCol, err = mergeAndCoerceCollations(input, pat)
	if err != nil {
		return nil, err
	}

	repl, ok := replArg.(*evalBytes)
	if !ok {
		repl, err = evalToVarchar(replArg, typedCol.Collation, true)
		if err != nil {
			return nil, err
		}
	}

	replRunes := charset.Expand(nil, repl.ToRawBytes(), repl.col.Collation.Get().Charset())

	var flags icuregex.RegexpFlag
	var collation = typedCol.Collation.Get()
	if strings.Contains(collation.Name(), "_ci") {
		flags |= icuregex.CaseInsensitive
	}

	pos := int64(1)
	occurrence := int64(0)
	inputRunes := charset.Expand(nil, input.ToRawBytes(), collation.Charset())

	if len(r.Arguments) > 3 {
		pos, err = evalPosition(env, r.Arguments[3], int64(len(inputRunes)))
		if err != nil {
			return nil, err
		}
	}

	if len(r.Arguments) > 4 {
		occurrence, err = evalOccurrence(env, r.Arguments[4])
		if err != nil {
			return nil, err
		}
	}

	if len(r.Arguments) > 5 {
		flags, err = evalRegexpFlags(env, r.Arguments[5], flags)
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
	if occurrence > 0 {
		for i := int64(0); i < occurrence; i++ {
			found, err = m.Find()
			if err != nil {
				return nil, err
			}
			if !found {
				break
			}
		}
		if !found {
			return newEvalRaw(sqltypes.Text, input.ToRawBytes(), typedCol), nil
		}

		out := append(inputRunes[:int64(m.Start())+pos-1], replRunes...)
		out = append(out, inputRunes[int64(m.End())+pos-1:]...)
		bytes := charset.Collapse(nil, out, collation.Charset())
		return newEvalRaw(sqltypes.Text, bytes, typedCol), nil
	}

	found, err = m.Find()
	if err != nil {
		return nil, err
	}

	if !found {
		return newEvalRaw(sqltypes.Text, input.ToRawBytes(), typedCol), nil
	}

	start := int64(m.Start()) + pos - 1
	out := append(inputRunes[:start], replRunes...)
	end := int64(m.End()) + pos - 1
	for {
		found, err = m.Find()
		if err != nil {
			return nil, err
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

	bytes := charset.Collapse(nil, out, collation.Charset())
	return newEvalRaw(sqltypes.Text, bytes, typedCol), nil
}

func (r *builtinRegexpReplace) typeof(env *ExpressionEnv, fields []*querypb.Field) (sqltypes.Type, typeFlag) {
	_, f1 := r.Arguments[0].typeof(env, fields)
	_, f2 := r.Arguments[1].typeof(env, fields)
	_, f3 := r.Arguments[2].typeof(env, fields)
	return sqltypes.Text, f1 | f2 | f3
}

func (r *builtinRegexpReplace) compile(c *compiler) (ctype, error) {
	return ctype{}, c.unsupported(r)
}

var _ Expr = (*builtinRegexpReplace)(nil)
