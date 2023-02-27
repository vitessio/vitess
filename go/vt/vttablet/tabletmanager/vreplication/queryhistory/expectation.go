package queryhistory

import "fmt"

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

	return fmt.Sprintf("query=%s", e.query)
}
