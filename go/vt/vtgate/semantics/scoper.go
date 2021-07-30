/*
Copyright 2021 The Vitess Authors.

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

func (s *scoper) push(sc *scope) {
	s.scopes = append(s.scopes, sc)
}

func (s *scoper) popScope() {
	l := len(s.scopes) - 1
	s.scopes = s.scopes[:l]
}
