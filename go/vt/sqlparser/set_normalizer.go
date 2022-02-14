package sqlparser

import (
	"strings"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type setNormalizer struct {
	err error
}

func (n *setNormalizer) rewriteSetComingUp(cursor *Cursor) bool {
	set, ok := cursor.node.(*Set)
	if ok {
		for i, expr := range set.Exprs {
			exp, err := n.normalizeSetExpr(expr)
			if err != nil {
				n.err = err
				return false
			}
			set.Exprs[i] = exp
		}
	}
	return true
}

func (n *setNormalizer) normalizeSetExpr(in *SetExpr) (*SetExpr, error) {
	switch in.Name.at { // using switch so we can use break
	case DoubleAt:
		if in.Scope != ImplicitScope {
			return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "cannot use scope and @@")
		}
		switch {
		case strings.HasPrefix(in.Name.Lowered(), "session."):
			in.Name = createColumn(in.Name.Lowered()[8:])
			in.Scope = SessionScope
		case strings.HasPrefix(in.Name.Lowered(), "global."):
			in.Name = createColumn(in.Name.Lowered()[7:])
			in.Scope = GlobalScope
		case strings.HasPrefix(in.Name.Lowered(), "vitess_metadata."):
			in.Name = createColumn(in.Name.Lowered()[16:])
			in.Scope = VitessMetadataScope
		default:
			in.Name.at = NoAt
			in.Scope = SessionScope
		}
		return in, nil
	case SingleAt:
		if in.Scope != ImplicitScope {
			return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "cannot mix scope and user defined variables")
		}
		return in, nil
	case NoAt:
		switch in.Scope {
		case ImplicitScope:
			in.Scope = SessionScope
		case LocalScope:
			in.Scope = SessionScope
		}
		return in, nil
	}
	panic("this should never happen")
}

func createColumn(str string) ColIdent {
	size := len(str)
	if str[0] == '`' && str[size-1] == '`' {
		str = str[1 : size-1]
	}
	return NewColIdent(str)
}
