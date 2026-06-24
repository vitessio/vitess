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

// Expect generates a sequence of expectations, where each query after the head
// query immediately follows the preceding query.
func Expect(head string, tail ...string) ExpectationSequencer {
	return Immediately(head, tail...)(nil)
}

// ExpectNone generates an empty sequence of expectations.
func ExpectNone() ExpectationSequence {
	return &expectationSequence{}
}

// Eventually generates an ExpectationSequencerFn which can be used to append a
// new sequence of expectations onto an existing sequence.
//
//	Expect("foo", "bar").Then(Eventually("hello", "world")
//
// Generates a sequence of expectations such that:
//
//   - "foo" is expected first
//   - "bar" immediately follows "foo"
//   - "hello" eventually follows "bar"
//   - "world" eventually follows "hello"
func Eventually(head string, tail ...string) ExpectationSequencerFn {
	return func(sequencer ExpectationSequencer) ExpectationSequencer {
		current := Query(head)
		var head, last SequencedExpectation
		if sequencer != nil && sequencer.Current() != nil {
			head = sequencer.Head()
			sequencer.Current().ExpectEventuallyBefore(current)
		} else {
			head = current
		}
		for _, q := range tail {
			last = current
			current = Query(q)
			last.ExpectEventuallyBefore(current)
		}
		return &expectationSequencer{
			ExpectationSequence: &expectationSequence{head},
			current:             current,
		}
	}
}

// Immediately generates an ExpectationSequencerFn which can be used to append a
// new sequence of expectations onto an existing sequence.
//
//	Expect("foo", "bar").Then(Immediately("hello", "world")
//
// Generates a sequence of expectations such that:
//
//   - "foo" is expected first
//   - "bar" immediately follows "foo"
//   - "hello" immediately follows "bar"
//   - "world" immediately follows "hello"
func Immediately(head string, tail ...string) ExpectationSequencerFn {
	return func(sequencer ExpectationSequencer) ExpectationSequencer {
		current := Query(head)
		var head, last SequencedExpectation
		if sequencer != nil && sequencer.Current() != nil {
			head = sequencer.Head()
			sequencer.Current().ExpectImmediatelyBefore(current)
		} else {
			head = current
		}
		for _, q := range tail {
			last = current
			current = Query(q)
			last.ExpectImmediatelyBefore(current)
		}
		return &expectationSequencer{
			ExpectationSequence: &expectationSequence{head},
			current:             current,
		}
	}
}

// Query generates a single-member expectation sequence.
func Query(query string) SequencedExpectation {
	return newSequencedExpectation(newExpectation(query))
}
