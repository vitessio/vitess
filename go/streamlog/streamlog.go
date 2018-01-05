/*
Copyright 2017 Google Inc.

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

// Package streamlog provides a non-blocking message broadcaster.
package streamlog

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/stats"
)

var (
	// RedactDebugUIQueries controls whether full queries and bind variables are suppressed from debug UIs.
	RedactDebugUIQueries = flag.Bool("redact-debug-ui-queries", false, "redact full queries and bind variables from debug UI")

	// QueryLogFormat controls the format of the query log (either text or json)
	QueryLogFormat = flag.String("querylog-format", "text", "format for query logs (\"text\" or \"json\")")

	sendCount         = stats.NewCounters("StreamlogSend")
	deliveredCount    = stats.NewMultiCounters("StreamlogDelivered", []string{"Log", "Subscriber"})
	deliveryDropCount = stats.NewMultiCounters("StreamlogDeliveryDroppedMessages", []string{"Log", "Subscriber"})
)

const (
	// QueryLogFormatText is the format specifier for text querylog output
	QueryLogFormatText = "text"

	// QueryLogFormatJSON is the format specifier for json querylog output
	QueryLogFormatJSON = "json"
)

// StreamLogger is a non-blocking broadcaster of messages.
// Subscribers can use channels or HTTP.
type StreamLogger struct {
	name       string
	size       int
	mu         sync.Mutex
	subscribed map[chan interface{}]string
}

// New returns a new StreamLogger that can stream events to subscribers.
// The size parameter defines the channel size for the subscribers.
func New(name string, size int) *StreamLogger {
	return &StreamLogger{
		name:       name,
		size:       size,
		subscribed: make(map[chan interface{}]string),
	}
}

// Send sends message to all the writers subscribed to logger. Calling
// Send does not block.
func (logger *StreamLogger) Send(message interface{}) {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	for ch, name := range logger.subscribed {
		select {
		case ch <- message:
			deliveredCount.Add([]string{logger.name, name}, 1)
		default:
			deliveryDropCount.Add([]string{logger.name, name}, 1)
		}
	}
	sendCount.Add(logger.name, 1)
}

// Subscribe returns a channel which can be used to listen
// for messages.
func (logger *StreamLogger) Subscribe(name string) chan interface{} {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	ch := make(chan interface{}, logger.size)
	logger.subscribed[ch] = name
	return ch
}

// Unsubscribe removes the channel from the subscription.
func (logger *StreamLogger) Unsubscribe(ch chan interface{}) {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	delete(logger.subscribed, ch)
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

		// Notify client that we're set up. Helpful to distinguish low-traffic streams from connection issues.
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()

		for message := range ch {
			if _, err := io.WriteString(w, messageFmt(r.Form, message)); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
	})
	log.Infof("Streaming logs from %s at %v.", logger.Name(), url)
}

// LogToFile starts logging to the specified file path and will reopen the
// file in response to SIGUSR2.
//
// Returns the channel used for the subscription which can be used to close
// it.
func (logger *StreamLogger) LogToFile(path string, messageFmt func(url.Values, interface{}) string) (chan interface{}, error) {
	rotateChan := make(chan os.Signal, 1)
	signal.Notify(rotateChan, syscall.SIGUSR2)

	logChan := logger.Subscribe("FileLog")
	formatParams := map[string][]string{"full": {}}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case record, _ := <-logChan:
				formatted := messageFmt(formatParams, record)
				f.WriteString(formatted)
			case _, _ = <-rotateChan:
				f.Close()
				f, _ = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
			}
		}
	}()

	return logChan, nil
}

// Formatter is a simple interface for objects that expose a Format function
// as needed for streamlog.
type Formatter interface {
	Format(url.Values) string
}

// GetFormatter returns a formatter function for objects conforming to the
// Formatter interface
func GetFormatter(logger *StreamLogger) func(url.Values, interface{}) string {
	return func(params url.Values, val interface{}) string {
		fmter, ok := val.(Formatter)
		if !ok {
			return fmt.Sprintf("Error: unexpected value of type %T in %s!", val, logger.Name())
		}
		return fmter.Format(params)
	}
}
