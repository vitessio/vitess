package queryhistory

// SequencedExpectationSet provides a set-like interface over a Golang map of
// SequencedExpectations.
type SequencedExpectationSet interface {
	Add(SequencedExpectation)
	Contains(SequencedExpectation) bool
	Slice() []SequencedExpectation
}

type sequencedExpectationSet map[SequencedExpectation]any

func (ses *sequencedExpectationSet) Add(expectation SequencedExpectation) {
	if ses == nil {
		*ses = make(sequencedExpectationSet)
	}
	(*ses)[expectation] = true
}

func (ses *sequencedExpectationSet) Contains(expectation SequencedExpectation) bool {
	if ses == nil {
		return false
	}
	_, c := (*ses)[expectation]
	return c
}

func (ses *sequencedExpectationSet) Slice() []SequencedExpectation {
	s := make([]SequencedExpectation, 0)
	if len(*ses) == 0 {
		return s
	}
	for se := range *ses {
		s = append(s, se)
	}
	return s
}
