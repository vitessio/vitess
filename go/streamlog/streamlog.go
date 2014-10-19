// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package streamlog provides a non-blocking message broadcaster.
package streamlog

import (
	"io"
	"net/http"
	"net/url"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
)

var (
	internalDropCount = stats.NewCounters("StreamlogInternallyDroppedMessages")
	deliveryDropCount = stats.NewMultiCounters("StreamlogDeliveryDroppedMessages", []string{"Log", "Subscriber"})
)

type subscriber struct {
	name string
}

// StreamLogger is a non-blocking broadcaster of messages.
// Subscribers can use channels or HTTP.
type StreamLogger struct {
	name       string
	dataQueue  chan interface{}
	mu         sync.Mutex
	subscribed map[chan interface{}]subscriber
	// size is used to check if there are any subscriptions. Keep
	// it atomically in sync with the size of subscribed.
	size sync2.AtomicUint32
}

// New returns a new StreamLogger with a buffer that can contain size
// messages. Any messages sent to it will be available at url.
func New(name string, size int) *StreamLogger {
	logger := &StreamLogger{
		name:       name,
		dataQueue:  make(chan interface{}, size),
		subscribed: make(map[chan interface{}]subscriber),
	}
	go logger.stream()
	return logger
}

// Send sends message to all the writers subscribed to logger. Calling
// Send does not block.
func (logger *StreamLogger) Send(message interface{}) {
	if logger.size.Get() == 0 {
		// There are no subscribers, do nothing.
		return
	}
	select {
	case logger.dataQueue <- message:
	default:
		internalDropCount.Add(logger.name, 1)
	}
}

// Subscribe returns a channel which can be used to listen
// for messages.
func (logger *StreamLogger) Subscribe(name string) chan interface{} {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	ch := make(chan interface{}, 1)
	logger.subscribed[ch] = subscriber{name: name}
	logger.size.Set(uint32(len(logger.subscribed)))
	return ch
}

// Unsubscribe removes the channel from the subscription.
func (logger *StreamLogger) Unsubscribe(ch chan interface{}) {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	delete(logger.subscribed, ch)
	logger.size.Set(uint32(len(logger.subscribed)))
}

// stream sends messages sent to logger to all of its subscribed
// writers. This method should be called in a goroutine.
func (logger *StreamLogger) stream() {
	for message := range logger.dataQueue {
		logger.transmit(message)
	}
}

func (logger *StreamLogger) transmit(message interface{}) {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	for ch, sub := range logger.subscribed {
		select {
		case ch <- message:
		default:
			deliveryDropCount.Add([]string{logger.name, sub.name}, 1)
		}
	}
}

// Name returns the name of StreamLogger.
func (logger *StreamLogger) Name() string {
	return logger.name
}

// ServeLogs registers the URL on which messages will be broadcast.
// It is safe to register multiple URLs for the same StreamLogger.
func (logger *StreamLogger) ServeLogs(url string, messageFmt func(url.Values, interface{}) string) {
	http.HandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
		if err := acl.CheckAccessHTTP(r, acl.DEBUGGING); err != nil {
			acl.SendError(w, err)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		ch := logger.Subscribe("ServeLogs")
		defer logger.Unsubscribe(ch)

		for message := range ch {
			if _, err := io.WriteString(w, messageFmt(r.Form, message)); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
	})
	log.Infof("Streaming logs from %s at %v.", logger.Name(), url)
}
