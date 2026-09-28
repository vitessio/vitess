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

// ExpectationSequencer is a convenient way to compose ExpectationSequences.
type ExpectationSequencer interface {
	ExpectationSequence
	// Return the current SequencedExpectation.
	Current() SequencedExpectation
	// Eventually returns an ExpectationSequencerFn that can be used to compose
	// this ExpectationSequencer with another.
	//
	// For example...
	//	sequencer1.Then(sequencer2.Eventually())
	//
	// Produces an ExpectationSequence that starts with sequence1, and is
	// eventually followed by the head of sequence2.
	Eventually() ExpectationSequencerFn
	// Immediately returns an ExpectationSequencerFn that can be used to
	// compose this ExpectationSequencer with another.
	//
	// For example...
	//	sequencer1.Then(sequencer2.Immediately())
	//
	// Produces an ExpectationSequence that starts with sequence1, and is
	// immediately followed by the head of sequence2.
	Immediately() ExpectationSequencerFn
	// Then passes this ExpectationSequencer to ExpectationSequencerFn,
	// and returns the resulting ExpectationSequencer.
	Then(ExpectationSequencerFn) ExpectationSequencer
}

type ExpectationSequencerFn func(ExpectationSequencer) ExpectationSequencer

type expectationSequencer struct {
	ExpectationSequence
	current SequencedExpectation
}

func (es *expectationSequencer) Current() SequencedExpectation {
	return es.current
}

func (es *expectationSequencer) Eventually() ExpectationSequencerFn {
	return func(parent ExpectationSequencer) ExpectationSequencer {
		es.Current().ExpectEventuallyAfter(parent.Current())
		return es
	}
}

func (es *expectationSequencer) Immediately() ExpectationSequencerFn {
	return func(parent ExpectationSequencer) ExpectationSequencer {
		es.Current().ExpectImmediatelyAfter(parent.Current())
		return es
	}
}

func (es *expectationSequencer) Then(then ExpectationSequencerFn) ExpectationSequencer {
	return then(es)
}
