/*
Copyright 2020 The Vitess Authors.

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

import (
	"fmt"
	"sort"
	"strings"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// QueryMatchesTemplates sees if the given query has the same fingerprint as one of the given templates
// (one is enough)
func (p *Parser) QueryMatchesTemplates(query string, queryTemplates []string) (match bool, err error) {
	if len(queryTemplates) == 0 {
		return false, fmt.Errorf("No templates found")
	}
	bv := make(map[string]*querypb.BindVariable)

	normalize := func(q string) (string, error) {
		q, err := p.NormalizeAlphabetically(q)
		if err != nil {
			return "", err
		}
		stmt, reservedVars, err := p.Parse2(q)
		if err != nil {
			return "", err
		}
		err = Normalize(stmt, NewReservedVars("", reservedVars), bv)
		if err != nil {
			return "", err
		}
		normalized := CanonicalString(stmt)
		return normalized, nil
	}

	normalizedQuery, err := normalize(query)
	if err != nil {
		return false, err
	}

	for _, template := range queryTemplates {
		normalizedTemplate, err := normalize(template)
		if err != nil {
			return false, err
		}

		// compare!
		if normalizedTemplate == normalizedQuery {
			return true, nil
		}
	}
	return false, nil
}

// NormalizeAlphabetically rewrites given query such that:
// - WHERE 'AND' expressions are reordered alphabetically
func (p *Parser) NormalizeAlphabetically(query string) (normalized string, err error) {
	stmt, err := p.Parse(query)
	if err != nil {
		return normalized, err
	}
	var where *Where
	switch stmt := stmt.(type) {
	case *Update:
		where = stmt.Where
	case *Delete:
		where = stmt.Where
	case *Select:
		where = stmt.Where
	}
	if where != nil {
		andExprs := SplitAndExpression(nil, where.Expr)
		sort.SliceStable(andExprs, func(i, j int) bool {
			return String(andExprs[i]) < String(andExprs[j])
		})
		var newWhere *Where
		for _, expr := range andExprs {
			if newWhere == nil {
				newWhere = &Where{
					Type: WhereClause,
					Expr: expr,
				}
			} else {
				newWhere.Expr = &AndExpr{
					Left:  newWhere.Expr,
					Right: expr,
				}
			}
		}
		switch stmt := stmt.(type) {
		case *Update:
			stmt.Where = newWhere
		case *Delete:
			stmt.Where = newWhere
		case *Select:
			stmt.Where = newWhere
		}
	}
	return String(stmt), nil
}

// ReplaceTableQualifiers takes a statement's table expressions and
// replaces any cases of the provided database name with the
// specified replacement name.
// Note: both database names provided should be unescaped strings.
func (p *Parser) ReplaceTableQualifiers(query, olddb, newdb string) (string, error) {
	if newdb == olddb {
		// Nothing to do here.
		return query, nil
	}
	in, err := p.Parse(query)
	if err != nil {
		return "", err
	}

	oldQualifier := NewIdentifierCS(olddb)
	newQualifier := NewIdentifierCS(newdb)

	modified := false
	upd := Rewrite(in, func(cursor *Cursor) bool {
		switch node := cursor.Node().(type) {
		case TableName:
			if node.Qualifier.NotEmpty() &&
				node.Qualifier.String() == oldQualifier.String() {
				node.Qualifier = newQualifier
				cursor.Replace(node)
				modified = true
			}
		case *ShowBasic: // for things like 'show tables from _vt'
			if node.DbName.NotEmpty() &&
				node.DbName.String() == oldQualifier.String() {
				node.DbName = newQualifier
				cursor.Replace(node)
				modified = true
			}
		}
		return true
	}, nil)
	// If we didn't modify anything, return the original query.
	// This is particularly helpful with unit tests that
	// execute a query which slightly differs from the parsed
	// version: e.g. 'where id=1' becomes 'where id = 1'.
	if modified {
		return String(upd), nil
	}
	return query, nil
}

// ReplaceTableQualifiersMultiQuery accepts a multi-query string and modifies it
// via ReplaceTableQualifiers, one query at a time.
func (p *Parser) ReplaceTableQualifiersMultiQuery(multiQuery, olddb, newdb string) (string, error) {
	queries, err := p.SplitStatementToPieces(multiQuery)
	if err != nil {
		return multiQuery, err
	}
	var modifiedQueries []string
	for _, query := range queries {
		// Replace any provided sidecar database qualifiers with the correct one.
		query, err := p.ReplaceTableQualifiers(query, olddb, newdb)
		if err != nil {
			return query, err
		}
		modifiedQueries = append(modifiedQueries, query)
	}
	return strings.Join(modifiedQueries, ";"), nil
}
