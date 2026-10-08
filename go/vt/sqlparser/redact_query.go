/*
Copyright 2019 The Vitess Authors.

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

package sqlparser

import querypb "vitess.io/vitess/go/vt/proto/query"

// RedactSQLQuery returns a sql string with the params stripped out for display.
// Comments are kept as written.
func (p *Parser) RedactSQLQuery(sql string) (string, error) {
	return p.redactSQLQuery(sql, false)
}

// RedactSQLQueryWithoutComments is like RedactSQLQuery, but it also drops the
// comments, whose text can hold anything.
func (p *Parser) RedactSQLQueryWithoutComments(sql string) (string, error) {
	return p.redactSQLQuery(sql, true)
}

func (p *Parser) redactSQLQuery(sql string, dropComments bool) (string, error) {
	bv := map[string]*querypb.BindVariable{}
	sqlStripped, comments := SplitMarginComments(sql)

	stmt, reservedVars, err := p.Parse2(sqlStripped)
	if err != nil {
		return "", err
	}

	out, err := Normalize(stmt, NewReservedVars("redacted", reservedVars), bv, true, "ks", 0, "", map[string]string{}, nil, nil)
	if err != nil {
		return "", err
	}

	if dropComments {
		_ = Walk(func(node SQLNode) (bool, error) {
			if commented, ok := node.(Commented); ok {
				commented.SetComments(nil)
			}
			return true, nil
		}, out.AST)
		return String(out.AST), nil
	}

	return comments.Leading + String(out.AST) + comments.Trailing, nil
}
