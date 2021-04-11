/*
Copyright 2021 The Vitess Authors.

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

package tabletserver

import (
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

const streamBufferSize = 8

// StreamConsolidator is a data structure capable of merging several identical streaming queries so only
// one query is executed in MySQL and its response is fanned out to all the clients simultaneously.
type StreamConsolidator struct {
	mu                             sync.Mutex
	inflight                       map[string]*streamInFlight
	memory                         int64
	maxMemoryTotal, maxMemoryQuery int64
	blocking                       bool
	cleanup                        StreamCallback
}

// NewStreamConsolidator allocates a stream consolidator. The consolidator will use up to maxMemoryTotal
// bytes in order to allow simultaneous queries to "catch up" to each other. Each individual stream will
// only use up to maxMemoryQuery bytes of memory as a history buffer to catch up.
func NewStreamConsolidator(maxMemoryTotal, maxMemoryQuery int64, cleanup StreamCallback) *StreamConsolidator {
	return &StreamConsolidator{
		inflight:       make(map[string]*streamInFlight),
		maxMemoryTotal: maxMemoryTotal,
		maxMemoryQuery: maxMemoryQuery,
		blocking:       false,
		cleanup:        cleanup,
	}
}

// StreamCallback is a function that is called with every Result object from a streaming query
type StreamCallback func(result *sqltypes.Result) error

// SetBlocking sets whether fanning out should block to wait for slower clients to
// catch up, or should immediately disconnect clients that are taking too long to process the
// consolidated stream. By default, blocking is only enabled when running with the race detector.
func (sc *StreamConsolidator) SetBlocking(block bool) {
	sc.blocking = block
}

// Consolidate wraps the execution of a streaming query so that any other queries being executed
// simultaneously will wait for the results of the original query, instead of being executed from
// scratch in MySQL.
// Query consolidation is based by comparing the resulting `sql` string, which should not contain
// comments in it. The original `callback` that will yield results to the client must be passed as
// `callback`. A `leaderCallback` must also be supplied: this function must perform the actual
// query in the upstream MySQL server, yielding results into the modified callback that it receives
// as an argument.
func (sc *StreamConsolidator) Consolidate(logStats *tabletenv.LogStats, sql string, callback StreamCallback, leaderCallback func(StreamCallback) error) error {
	var (
		inflight        *streamInFlight
		catchup         []*sqltypes.Result
		followChan      chan *sqltypes.Result
		err             error
		leaderClientErr error
	)

	sc.mu.Lock()
	// check if we have an existing identical query in our consolidation table
	inflight = sc.inflight[sql]

	// if there's an existing stream for our query, try to follow it
	if inflight != nil {
		catchup, followChan = inflight.follow()
	}

	// if there isn't an existing stream; OR if there is an existing stream but
	// we're too late to catch up to it, we declare ourselves the leader for this query
	if inflight == nil || followChan == nil {
		inflight = &streamInFlight{
			catchupAllowed: true,
		}
		sc.inflight[sql] = inflight
	}
	sc.mu.Unlock()

	// if we have a followChan, we're following up on a query that is already being served
	if followChan != nil {
		defer func() {
			memchange := inflight.unfollow(followChan, sc.cleanup)
			atomic.AddInt64(&sc.memory, memchange)
		}()

		logStats.QuerySources |= tabletenv.QuerySourceConsolidator

		// first, catch up our client by sending all the Results to the streaming query
		// that the leader has already sent
		for _, result := range catchup {
			if err := callback(result); err != nil {
				return err
			}
		}

		// now we can follow the leader: it will send in real time all new Results through
		// our follower channel
		for result := range followChan {
			if err := callback(result); err != nil {
				return err
			}
		}

		// followChan has been closed by the leader, so there are no more results to send.
		// check the final error return for the stream
		return inflight.result(followChan)
	}

	// we don't have a followChan so we're the leaders for this query. we must run it in the
	// upstream MySQL and fan out all the Results to any followers that show up

	defer func() {
		sc.mu.Lock()
		// only remove ourselves from the in-flight streams map if we're still there;
		// if our stream has been running for too long so that new followers wouldn't be able
		// to catch up, a follower may have replaced us in the map.
		if existing := sc.inflight[sql]; existing == inflight {
			delete(sc.inflight, sql)
		}
		sc.mu.Unlock()

		// finalize the stream with the error return we got from the leaderCallback
		memchange := inflight.finishLeader(err, sc.cleanup)
		atomic.AddInt64(&sc.memory, memchange)
	}()

	// leaderCallback will perform the actual streaming query in MySQL; we provide it a custom
	// results callback so that we can intercept the results as they come in
	err = leaderCallback(func(result *sqltypes.Result) error {
		// update the live consolidated stream; this will fan out the Result to all our active followers
		// and tell us how much more memory we're using by temporarily storing the result so other followers
		// in the future can catch up to this stream
		memChange := inflight.update(result, sc.blocking, sc.maxMemoryQuery, sc.maxMemoryTotal-atomic.LoadInt64(&sc.memory))
		atomic.AddInt64(&sc.memory, memChange)

		// yield the result to the very first client that started the query; this client is not listening
		// on a follower channel.
		if leaderClientErr == nil {
			// if our leader client returns an error from the callback, we do NOT want to send it upstream,
			// because that would cancel the stream from MySQL. Keep track of the error so we can return it
			// once we've finished the stream for all our followers UNLESS we currently have 0 active followers;
			// if that's the case, we can terminate early.
			leaderClientErr = callback(result)
			if leaderClientErr != nil && !inflight.shouldContinueStreaming() {
				return leaderClientErr
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return leaderClientErr
}

type streamInFlight struct {
	mu             sync.Mutex
	catchup        []*sqltypes.Result
	fanout         map[chan *sqltypes.Result]bool
	err            error
	memory         int64
	catchupAllowed bool
	finished       bool
}

// follow adds a follower to this in-flight stream, returning a slice with all
// the Results that have been sent so far (so the client can catch up) and a channel
// that will receive all the Results in the future.
// If this stream has been running for too long and we cannot catch up to it, follow
// returns a nil channel.
func (s *streamInFlight) follow() ([]*sqltypes.Result, chan *sqltypes.Result) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.catchupAllowed {
		return nil, nil
	}
	if s.fanout == nil {
		s.fanout = make(map[chan *sqltypes.Result]bool)
	}
	follow := make(chan *sqltypes.Result, streamBufferSize)
	s.fanout[follow] = true
	return s.catchup, follow
}

// unfollow unsubscribes the given follower from receiving more results from the stream.
func (s *streamInFlight) unfollow(ch chan *sqltypes.Result, cleanup StreamCallback) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.fanout, ch)
	return s.checkFollowers(cleanup)
}

// result returns the final error for this stream. If the stream finished successfully,
// this is nil. If the stream had an upstream error (i.e. from MySQL), this error is
// returned. Lastly, if this specific follower had an error that caused it to fall behind
// from the consolidation stream, a specific error is returned.
func (s *streamInFlight) result(ch chan *sqltypes.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	alive := s.fanout[ch]
	if !alive {
		return vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "stream lagged behind during consolidation")
	}
	return s.err
}

// shouldContinueStreaming returns whether this stream has active followers;
// if it doesn't, it marks the stream as terminated.
func (s *streamInFlight) shouldContinueStreaming() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.fanout) > 0 {
		return true
	}
	s.catchupAllowed = false
	s.catchup = nil
	return false
}

// update fans out the given result to all the active followers for the stream and
// returns the amount of memory that is being used by the catchup buffer
func (s *streamInFlight) update(result *sqltypes.Result, block bool, maxMemoryQuery, maxMemoryTotal int64) int64 {
	var memoryChange int64
	resultSize := result.CachedSize(true)

	s.mu.Lock()
	defer s.mu.Unlock()

	// if this stream can still be catched up with, we need to store the result in
	// a catch up buffer; otherwise, we can skip this altogether and just fan out the result
	// to all the followers that are already caught up
	if s.catchupAllowed {
		if s.memory+resultSize > maxMemoryQuery || resultSize > maxMemoryTotal {
			// if the catch up buffer has grown too large, disable catching up to this stream.
			s.catchupAllowed = false
		} else {
			// otherwise store the result in our catchup buffer for future clients
			s.catchup = append(s.catchup, result)
			s.memory += resultSize
			memoryChange = resultSize
		}
	}

	if block {
		for follower := range s.fanout {
			follower <- result
		}
	} else {
		// fan out the result to all the followers that are currently active
		for follower, alive := range s.fanout {
			if alive {
				select {
				case follower <- result:
				default:
					// if we cannot write to this follower's channel, it means its client is taking
					// too long to relay the stream; we must drop it from our our consolidation. the
					// client will receive an error.
					s.fanout[follower] = false
					close(follower)
				}
			}
		}
	}

	return memoryChange
}

// finishLeader terminates this consolidated stream by storing the final error result from
// MySQL and notifying all the followers that there are no more Results left to be sent
func (s *streamInFlight) finishLeader(err error, cleanup StreamCallback) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.err = err
	s.finished = true
	for follower, alive := range s.fanout {
		if alive {
			close(follower)
		}
	}
	return s.checkFollowers(cleanup)
}

func (s *streamInFlight) checkFollowers(cleanup StreamCallback) int64 {
	if s.finished && len(s.fanout) == 0 {
		for _, result := range s.catchup {
			_ = cleanup(result)
		}
		s.catchup = nil
		return -s.memory
	}
	return 0
}
