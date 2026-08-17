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
