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

// SequencedExpectation is an Expectation situated in an ExpectationSequence.
// In other words, it is an Expectation with temporal relationships to other
// Expectations.
type SequencedExpectation interface {
	Expectation

	// EventuallyAfter returns the SequencedExpectationSet containing
	// SequencedExpectations which this SequencedExpectation eventually
	// follows.
	EventuallyAfter() SequencedExpectationSet
	// EventuallyBefore returns the SequencedExpectationSet contains
	// SequencedExpectations eventually follow this SequencedExpectation.
	EventuallyBefore() SequencedExpectationSet
	// ExpectImmediatelyAfter sets the SequencedExpectation which this
	// SequencedExpectation immediately follows. It also sets the inverse
	// relationship on the provided expectation.
	ExpectImmediatelyAfter(SequencedExpectation)
	// ExpectImmediatelyBefore sets the SequencedExpectation which immediately
	// follow this SequencedExpectation. It also sets the inverse relationship
	// on the provided expectation.
	ExpectImmediatelyBefore(SequencedExpectation)
	// ExpectEventuallyAfter adds a SequencedExpectation to the
	// SequencedExpectationSet which this SequencedExpectation eventually
	// follows. It also sets the inverse relationship on the provided
	// expectation.
	ExpectEventuallyAfter(SequencedExpectation)
	// ExpectEventuallyAfter adds a SequencedExpectation to the
	// SequencedExpectationSet which eventually follows this
	// SequencedExpectation. It also sets the inverse relationship on the
	// provided expectation.
	ExpectEventuallyBefore(SequencedExpectation)
	// ImmediatelyAfter returns the SequencedExpectation which this
	// SequencedExpectation immediately follows.
	ImmediatelyAfter() SequencedExpectation
	// ImmediatelyBefore returns the SequencedExpectation which immediately
	// follows this SequencedExpectation.
	ImmediatelyBefore() SequencedExpectation
}

type sequencedExpectation struct {
	Expectation
	eventuallyAfter   SequencedExpectationSet
	eventuallyBefore  SequencedExpectationSet
	immediatelyAfter  SequencedExpectation
	immediatelyBefore SequencedExpectation
}

func newSequencedExpectation(expectation Expectation) SequencedExpectation {
	eventuallyAfter := sequencedExpectationSet(make(map[SequencedExpectation]any))
	eventuallyBefore := sequencedExpectationSet(make(map[SequencedExpectation]any))
	return &sequencedExpectation{
		Expectation:      expectation,
		eventuallyAfter:  &eventuallyAfter,
		eventuallyBefore: &eventuallyBefore,
	}
}

func (se *sequencedExpectation) EventuallyAfter() SequencedExpectationSet {
	return se.eventuallyAfter
}

func (se *sequencedExpectation) EventuallyBefore() SequencedExpectationSet {
	return se.eventuallyBefore
}

func (se *sequencedExpectation) ExpectEventuallyAfter(expectation SequencedExpectation) {
	if !se.eventuallyAfter.Contains(expectation) {
		se.eventuallyAfter.Add(expectation)
		expectation.ExpectEventuallyBefore(se)
	}
}

func (se *sequencedExpectation) ExpectEventuallyBefore(expectation SequencedExpectation) {
	if !se.eventuallyBefore.Contains(expectation) {
		se.eventuallyBefore.Add(expectation)
		expectation.ExpectEventuallyAfter(se)
	}
}

func (se *sequencedExpectation) ExpectImmediatelyAfter(expectation SequencedExpectation) {
	if se.immediatelyAfter != expectation {
		se.immediatelyAfter = expectation
		expectation.ExpectImmediatelyBefore(se)
	}
}

func (se *sequencedExpectation) ExpectImmediatelyBefore(expectation SequencedExpectation) {
	if se.immediatelyBefore != expectation {
		se.immediatelyBefore = expectation
		expectation.ExpectImmediatelyAfter(se)
	}
}

func (se *sequencedExpectation) ImmediatelyAfter() SequencedExpectation {
	return se.immediatelyAfter
}

func (se *sequencedExpectation) ImmediatelyBefore() SequencedExpectation {
	return se.immediatelyBefore
}
