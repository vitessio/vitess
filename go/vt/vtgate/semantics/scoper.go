package semantics

import "vitess.io/vitess/go/vt/sqlparser"

type scoper struct {
	rScope map[*sqlparser.Select]*scope
	wScope map[*sqlparser.Select]*scope
	scopes []*scope
}

func newScoper() *scoper {
	return &scoper{
		rScope: map[*sqlparser.Select]*scope{},
		wScope: map[*sqlparser.Select]*scope{},
	}
}

func (s *scoper) currentScope() *scope {
	size := len(s.scopes)
	if size == 0 {
		return nil
	}
	return s.scopes[size-1]
}
