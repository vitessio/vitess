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
