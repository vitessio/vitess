package queryhistory

// ExpectationSequence represents a temporal ordering of expectations.
type ExpectationSequence interface {
	Count() int
	// Head returns the head of the sequence. A sequence may only have one
	// head.
	Head() SequencedExpectation
	// Visit every expectation in the sequence, in any order.
	Visit(ExpectationSequenceVisitor)
}

type ExpectationSequenceVisitor func(SequencedExpectation) VisitControl

type VisitControl int

type expectationSequence struct {
	head SequencedExpectation
}

const (
	VisitContinue VisitControl = iota
	VisitTerminate
)

func (es *expectationSequence) Count() int {
	count := 0
	es.Visit(func(_ SequencedExpectation) VisitControl {
		count++
		return VisitContinue
	})
	return count
}

func (es *expectationSequence) Head() SequencedExpectation {
	return es.head
}

func (es *expectationSequence) Visit(visitor ExpectationSequenceVisitor) {
	next := make([]SequencedExpectation, 0)
	visited := make(map[SequencedExpectation]bool)

	if es.head != nil {
		next = append(next, es.head)
	}

	for len(next) > 0 {
		se := next[0]
		next = next[1:]

		if visited[se] {
			continue
		}

		control := visitor(se)

		switch control {
		case VisitTerminate:
			return
		default:
			visited[se] = true
		}

		if se.ImmediatelyBefore() != nil {
			next = append(next, se.ImmediatelyBefore())
		}

		next = append(next, se.EventuallyBefore().Slice()...)
	}
}
