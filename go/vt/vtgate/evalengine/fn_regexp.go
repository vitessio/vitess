package evalengine

import (
	"strings"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/icuregex"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type builtinRegexpLike struct {
	CallExpr
	Negate bool
}

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
				flags &= ^icuregex.UREGEX_CASE_INSENSITIVE
			case 'i':
				flags |= icuregex.UREGEX_CASE_INSENSITIVE
			case 'm':
				flags |= icuregex.UREGEX_MULTILINE
			case 'n':
				flags |= icuregex.UREGEX_DOTALL
			case 'u':
				flags |= icuregex.UREGEX_UNIX_LINES
			}
		}
	}

	return flags, nil
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

	var colid collations.ID
	input, pat, colid, err = mergeAndCoerceCollations(input, pat)
	if err != nil {
		return nil, err
	}

	var flags icuregex.RegexpFlag
	var collation = colid.Get()
	if strings.Contains(collation.Name(), "_ci") {
		flags |= icuregex.UREGEX_CASE_INSENSITIVE
	}

	if len(r.Arguments) > 2 {
		flags, err = evalRegexpFlags(env, r.Arguments[2], flags)
		if err != nil {
			return nil, err
		}
	}

	patUtf8, err := charset.Convert(nil, &charset.Charset_utf8mb4{}, pat.ToRawBytes(), collation.Charset())
	if err != nil {
		return nil, err
	}

	regexp, err := icuregex.CompileString(hack.String(patUtf8), flags)
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
	return sqltypes.Int64, f1 | f2
}

func (r *builtinRegexpLike) compile(c *compiler) (ctype, error) {
	return ctype{}, c.unsupported(r)
}

var _ Expr = (*builtinRegexpLike)(nil)
