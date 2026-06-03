package queryhistory

import (
	"fmt"
	"regexp"

	"vitess.io/vitess/go/mysql/replication"
)

var posUpdateRegex = regexp.MustCompile(`update _vt\.vreplication set pos='([^']*)'`)

// PosBetween returns an ExpectationSequencerFn that appends an expectation for
// an "update _vt.vreplication set pos=..." query whose recorded GTID position
// lies in the inclusive range [lower, upper], immediately following the current
// expectation in the sequence.
//
// It exists because a vplayer's stop position is the GTID of an event in the
// replication stream, while the bracketing positions are read from SHOW MASTER
// STATUS, which can advance independently due to background activity on the same
// server. Asserting set-containment instead of exact equality tolerates that lag
// and works for multi-source GTID sets.
//
//	qh.Expect("begin").
//		Then(qh.PosBetween(before, after)).
//		Then(qh.Immediately("/update _vt.vreplication set state='Stopped'", "commit"))
func PosBetween(lower, upper replication.Position) ExpectationSequencerFn {
	return func(sequencer ExpectationSequencer) ExpectationSequencer {
		current := newSequencedExpectation(&posBetweenExpectation{lower: lower, upper: upper})
		head := current
		if sequencer != nil && sequencer.Current() != nil {
			head = sequencer.Head()
			sequencer.Current().ExpectImmediatelyBefore(current)
		}
		return &expectationSequencer{
			ExpectationSequence: &expectationSequence{head},
			current:             current,
		}
	}
}

type posBetweenExpectation struct {
	lower replication.Position
	upper replication.Position
}

func (e *posBetweenExpectation) ExpectQuery(string) {}

func (e *posBetweenExpectation) MatchQuery(query string) (bool, error) {
	matches := posUpdateRegex.FindStringSubmatch(query)
	if matches == nil {
		return false, nil
	}
	stored, err := replication.DecodePosition(matches[1])
	if err != nil {
		return false, err
	}
	return stored.AtLeast(e.lower) && e.upper.AtLeast(stored), nil
}

func (e *posBetweenExpectation) Query() string {
	return e.String()
}

func (e *posBetweenExpectation) String() string {
	return fmt.Sprintf("update _vt.vreplication set pos in [%s, %s]",
		replication.EncodePosition(e.lower), replication.EncodePosition(e.upper))
}
