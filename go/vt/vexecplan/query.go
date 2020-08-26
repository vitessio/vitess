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

package vexecplan

import (
	"fmt"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

// QueryMatchesTemplates sees if the given query has the same fingerprint as one of the given templates
// (one is enough)
func QueryMatchesTemplates(query string, queryTemplates []string) (match bool, err error) {
	if len(queryTemplates) == 0 {
		return false, fmt.Errorf("No templates found")
	}
	bv := make(map[string]*querypb.BindVariable)

	normalize := func(q string) (string, error) {
		stmt, err := sqlparser.Parse(q)
		if err != nil {
			return "", err
		}
		sqlparser.Normalize(stmt, bv, "")
		normalized := sqlparser.String(stmt)
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
