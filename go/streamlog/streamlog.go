// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package streamlog

import (
	"io"
	"net/http"
	"net/url"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
)

var droppedMessages = stats.NewCounters("StreamlogDroppedMessages")

// StreamLogger is a non-blocking broadcaster of messages.
// Subscribers can use channels or HTTP.
type StreamLogger struct {
	name       string
	dataQueue  chan Formatter
	mu         sync.Mutex
	subscribed map[chan string]url.Values
	// size is used to check if there are any subscriptions. Keep
	// it atomically in sync with the size of subscribed.
	size sync2.AtomicUint32
}

// Formatter defines the interface that messages have to satisfy
// to be broadcast through StreamLogger.
type Formatter interface {
	Format(url.Values) string
}

// New returns a new StreamLogger with a buffer that can contain size
// messages. Any messages sent to it will be available at url.
func New(name string, size int) *StreamLogger {
	logger := &StreamLogger{
		name:       name,
		dataQueue:  make(chan Formatter, size),
		subscribed: make(map[chan string]url.Values),
	}
	go logger.stream()
	return logger
}

// ServeLogs registers the URL on which messages will be broadcast.
// It is safe to register multiple URLs for the same StreamLogger.
func (logger *StreamLogger) ServeLogs(url string) {
	http.Handle(url, logger)
	log.Infof("Streaming logs from %s at %v.", logger.Name(), url)
}

// Send sends message to all the writers subscribed to logger. Calling
// Send does not block.
func (logger *StreamLogger) Send(message Formatter) {
	if logger.size.Get() == 0 {
		// There are no subscribers, do nothing.
		return
	}
	select {
	case logger.dataQueue <- message:
	default:
		droppedMessages.Add(logger.name, 1)
	}
}

// Subscribe returns a channel which can be used to listen
// for messages.
func (logger *StreamLogger) Subscribe(params url.Values) chan string {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	ch := make(chan string, 1)
	logger.subscribed[ch] = params
	logger.size.Set(uint32(len(logger.subscribed)))
	return ch
}

// Unsubscribe removes the channel from the subscription.
func (logger *StreamLogger) Unsubscribe(ch chan string) {
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

func (logger *StreamLogger) transmit(message Formatter) {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	for ch, params := range logger.subscribed {
		messageString := message.Format(params)
		select {
		case ch <- messageString:
		default:
			droppedMessages.Add(logger.name, 1)
		}
	}
}

// Name returns the name of StreamLogger.
func (logger *StreamLogger) Name() string {
	return logger.name
}

// ServeHTTP is the http handler for StreamLogger.
func (logger *StreamLogger) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	ch := logger.Subscribe(r.Form)
	defer logger.Unsubscribe(ch)

	for messageString := range ch {
		if _, err := io.WriteString(w, messageString); err != nil {
			return
		}
		w.(http.Flusher).Flush()
	}
}
