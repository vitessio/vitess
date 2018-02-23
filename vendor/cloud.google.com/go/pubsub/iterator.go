// Copyright 2016 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pubsub

import (
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/api/support/bundler"
)

type MessageIterator struct {
	// kaTicker controls how often we send an ack deadline extension request.
	kaTicker *time.Ticker
	// ackTicker controls how often we acknowledge a batch of messages.
	ackTicker *time.Ticker

	ka     *keepAlive
	acker  *acker
	nacker *bundler.Bundler
	puller *puller

	// mu ensures that cleanup only happens once, and concurrent Stop
	// invocations block until cleanup completes.
	mu sync.Mutex

	// closed is used to signal that Stop has been called.
	closed chan struct{}
}

// newMessageIterator starts a new MessageIterator.  Stop must be called on the MessageIterator
// when it is no longer needed.
// subName is the full name of the subscription to pull messages from.
// ctx is the context to use for acking messages and extending message deadlines.
func newMessageIterator(ctx context.Context, s service, subName string, po *pullOptions) *MessageIterator {
	// TODO: make kaTicker frequency more configurable.
	// (ackDeadline - 5s) is a reasonable default for now, because the minimum ack period is 10s.  This gives us 5s grace.
	keepAlivePeriod := po.ackDeadline - 5*time.Second
	kaTicker := time.NewTicker(keepAlivePeriod) // Stopped in it.Stop

	// TODO: make ackTicker more configurable.  Something less than
	// kaTicker is a reasonable default (there's no point extending
	// messages when they could be acked instead).
	ackTicker := time.NewTicker(keepAlivePeriod / 2) // Stopped in it.Stop

	ka := &keepAlive{
		s:             s,
		Ctx:           ctx,
		Sub:           subName,
		ExtensionTick: kaTicker.C,
		Deadline:      po.ackDeadline,
		MaxExtension:  po.maxExtension,
	}

	ack := &acker{
		s:       s,
		Ctx:     ctx,
		Sub:     subName,
		AckTick: ackTicker.C,
		Notify:  ka.Remove,
	}

	nacker := bundler.NewBundler("", func(ackIDs interface{}) {
		// NACK by setting the ack deadline to zero, to make the message
		// immediately available for redelivery.
		//
		// If the RPC fails, nothing we can do about it. In the worst case, the
		// deadline for these messages will expire and they will still get
		// redelivered.
		_ = s.modifyAckDeadline(ctx, subName, 0, ackIDs.([]string))
	})
	nacker.DelayThreshold = keepAlivePeriod / 10 // nack promptly
	nacker.BundleCountThreshold = 10

	pull := newPuller(s, subName, ctx, po.maxPrefetch, ka.Add, ka.Remove)

	ka.Start()
	ack.Start()
	return &MessageIterator{
		kaTicker:  kaTicker,
		ackTicker: ackTicker,
		ka:        ka,
		acker:     ack,
		nacker:    nacker,
		puller:    pull,
		closed:    make(chan struct{}),
	}
}

// Next returns the next Message to be processed.  The caller must call
// Message.Done when finished with it.
// Once Stop has been called, calls to Next will return iterator.Done.
func (it *MessageIterator) Next() (*Message, error) {
	m, err := it.puller.Next()

	if err == nil {
		m.it = it
		return m, nil
	}

	select {
	// If Stop has been called, we return Done regardless the value of err.
	case <-it.closed:
		return nil, iterator.Done
	default:
		return nil, err
	}
}

// Client code must call Stop on a MessageIterator when finished with it.
// Stop will block until Done has been called on all Messages that have been
// returned by Next, or until the context with which the MessageIterator was created
// is cancelled or exceeds its deadline.
// Stop need only be called once, but may be called multiple times from
// multiple goroutines.
func (it *MessageIterator) Stop() {
	it.mu.Lock()
	defer it.mu.Unlock()

	select {
	case <-it.closed:
		// Cleanup has already been performed.
		return
	default:
	}

	// We close this channel before calling it.puller.Stop to ensure that we
	// reliably return iterator.Done from Next.
	close(it.closed)

	// Stop the puller. Once this completes, no more messages will be added
	// to it.ka.
	it.puller.Stop()

	// Start acking messages as they arrive, ignoring ackTicker.  This will
	// result in it.ka.Stop, below, returning as soon as possible.
	it.acker.FastMode()

	// This will block until
	//   (a) it.ka.Ctx is done, or
	//   (b) all messages have been removed from keepAlive.
	// (b) will happen once all outstanding messages have been either ACKed or NACKed.
	it.ka.Stop()

	// There are no more live messages, so kill off the acker.
	it.acker.Stop()
	it.nacker.Stop()
	it.kaTicker.Stop()
	it.ackTicker.Stop()
}

func (it *MessageIterator) done(ackID string, ack bool) {
	if ack {
		it.acker.Ack(ackID)
		// There's no need to call it.ka.Remove here, as acker will
		// call it via its Notify function.
	} else {
		it.ka.Remove(ackID)
		_ = it.nacker.Add(ackID, len(ackID)) // ignore error; this is just an optimization
	}
}
