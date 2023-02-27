package queryhistory

import (
	"fmt"
)

type Result struct {
	Accepted    bool
	Error       error
	Expectation Expectation
	Index       int
	Matched     bool
	Message     string
}

// Verifier verifies that an actual history of queries matches an expected
// sequence of queries.
type Verifier struct {
	matched  []SequencedExpectation
	pending  map[SequencedExpectation]bool
	history  History
	results  []*Result
	sequence ExpectationSequence
}

func NewVerifier(sequence ExpectationSequence) *Verifier {
	matched := make([]SequencedExpectation, 0)
	pending := make(map[SequencedExpectation]bool)
	sequence.Visit(func(e SequencedExpectation) VisitControl {
		pending[e] = true
		return VisitContinue
	})

	return &Verifier{
		matched:  matched,
		pending:  pending,
		history:  make([]string, 0),
		results:  make([]*Result, 0),
		sequence: sequence,
	}
}

// AcceptQuery verifies that the provided query is valid according to the
// internal ExpectationSequence and the internal History of preceeding queries.
// Returns a *Result indicating whether the query was accepted and, if not,
// diagnostic details indicating why not.
func (v *Verifier) AcceptQuery(query string) *Result {
	history := append(v.history, query)
	index := len(history) - 1
	result := &Result{
		Accepted:    false,
		Error:       nil,
		Expectation: nil,
		Index:       index,
		Matched:     false,
		Message:     "query did not match any expectations",
	}

	// Evaluate query against pending expectations.
	for e, p := range v.pending {
		// Continue to next expectation if no longer pending.
		if !p {
			continue
		}

		if v.checkQueryAgainstExpectation(query, e, result) {
			// Accept query into history.
			v.history = history
			v.matched = append(v.matched, e)
			v.pending[e] = false
			v.results = append(v.results, result)
			break
		}
	}

	return result
}

// History returns the internal History of accepted queries.
func (v *Verifier) History() History {
	return v.history
}

// Pending returns a list of Expectations that have not yet fully matched an
// accepted query.
func (v *Verifier) Pending() []Expectation {
	pending := make([]Expectation, 0)
	for e, p := range v.pending {
		if p {
			pending = append(pending, e)
		}
	}
	return pending
}

func (v *Verifier) checkQueryAgainstExpectation(query string, expectation SequencedExpectation, result *Result) bool {
	// Check if query matches expectation query pattern.
	ok, err := expectation.MatchQuery(query)
	if !ok {
		if err != nil {
			result.Expectation = expectation
			result.Error = err
			result.Message = "got an error while matching query to expectation"
			return false
		}
		// Query did not match, continue to next expectation.
		return false
	}

	// Query matched expectation query pattern.
	result.Message = "matched an expected query"
	result.Expectation = expectation

	// See if it matches sequence expectations.
	if expectation.ImmediatelyAfter() != nil {
		if len(v.matched) == 0 {
			result.Message = fmt.Sprintf(
				"%q expected immediately after %q, but it is first",
				query, expectation.ImmediatelyAfter().Query(),
			)
			return false
		}
		if v.pending[expectation.ImmediatelyAfter()] {
			result.Message = fmt.Sprintf(
				"expected immediately after %q which is not yet matched",
				expectation.ImmediatelyAfter().Query(),
			)
			return false
		}
	}
	for _, ea := range expectation.EventuallyAfter().Slice() {
		if v.pending[ea] {
			result.Message = fmt.Sprintf(
				"expect eventually after %q which is not yet matched",
				ea.Query(),
			)
			return false
		}
	}
	if expectation.ImmediatelyBefore() != nil {
		if !v.pending[expectation.ImmediatelyBefore()] {
			result.Message = fmt.Sprintf(
				"expected immediately before %q which is already matched",
				expectation.ImmediatelyBefore().Query(),
			)
			return false
		}
	}
	for _, ea := range expectation.EventuallyBefore().Slice() {
		if !v.pending[ea] {
			result.Message = fmt.Sprintf(
				"expect eventually before %q which is already matched",
				ea.Query(),
			)
			return false
		}
	}

	// Query passed expectation.
	result.Accepted = true
	result.Matched = true
	result.Message = "matched expectated query and expected order"

	return true
}
