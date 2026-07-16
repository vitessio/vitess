/*
Copyright 2026 The Vitess Authors.

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

package queryhistory

// Expectation represents an expectation about the contents of a query.
type Expectation interface {
	ExpectQuery(string)
	MatchQuery(string) (bool, error)
	Query() string
	String() string
}

type expectation struct {
	query string
}

func newExpectation(query string) Expectation {
	return &expectation{query}
}

func (e *expectation) ExpectQuery(query string) {
	e.query = query
}

func (e *expectation) MatchQuery(query string) (bool, error) {
	return MatchQueries(e.query, query)
}

func (e *expectation) Query() string {
	return e.query
}

func (e *expectation) String() string {
	if e == nil {
		return "<nil>"
	}

	return "query=" + e.query
}
