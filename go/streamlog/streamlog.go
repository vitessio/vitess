/*
Copyright 2019 The Vitess Authors.

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
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
)

var (
	sendCount      = stats.NewCountersWithSingleLabel("StreamlogSend", "stream log send count", "logger_names")
	deliveredCount = stats.NewCountersWithMultiLabels(
		"StreamlogDelivered",
		"Stream log delivered",
		[]string{"Log", "Subscriber"})
	deliveryDropCount = stats.NewCountersWithMultiLabels(
		"StreamlogDeliveryDroppedMessages",
		"Dropped messages by streamlog delivery",
		[]string{"Log", "Subscriber"})
)

const (
	// QueryLogFormatText is the format specifier for text querylog output
	QueryLogFormatText = "text"

	// QueryLogFormatJSON is the format specifier for json querylog output
	QueryLogFormatJSON = "json"

	// QueryLogModeAll is the mode specifier for logging all queries
	QueryLogModeAll = "all"

	// QueryLogModeError is the mode specifier for logging only queries that return an error
	QueryLogModeError = "error"
)

type QueryLogConfig struct {
	RedactDebugUIQueries bool
	FilterTag            string
	Format               string
	Mode                 string
	RowThreshold         uint64
	TimeThreshold        time.Duration
	sampleRate           float64
}

var queryLogConfigInstance = QueryLogConfig{
	Format: QueryLogFormatText,
	Mode:   QueryLogModeAll,
}

func GetQueryLogConfig() QueryLogConfig {
	return queryLogConfigInstance
}

func NewQueryLogConfigForTest() QueryLogConfig {
	return QueryLogConfig{
		Format: QueryLogFormatText,
	}
}

func init() {
	servenv.OnParseFor("vtcombo", registerStreamLogFlags)
	servenv.OnParseFor("vttablet", registerStreamLogFlags)
	servenv.OnParseFor("vtgate", registerStreamLogFlags)
}

func registerStreamLogFlags(fs *pflag.FlagSet) {
	// RedactDebugUIQueries controls whether full queries and bind variables are suppressed from debug UIs.
	fs.BoolVar(&queryLogConfigInstance.RedactDebugUIQueries, "redact-debug-ui-queries", queryLogConfigInstance.RedactDebugUIQueries, "redact full queries and bind variables from debug UI")

	// QueryLogFormat controls the format of the query log (either text or json)
	fs.StringVar(&queryLogConfigInstance.Format, "querylog-format", queryLogConfigInstance.Format, "format for query logs (\"text\" or \"json\")")

	// QueryLogFilterTag contains an optional string that must be present in the query for it to be logged
	fs.StringVar(&queryLogConfigInstance.FilterTag, "querylog-filter-tag", queryLogConfigInstance.FilterTag, "string that must be present in the query for it to be logged; if using a value as the tag, you need to disable query normalization")

	// QueryLogRowThreshold only log queries returning or affecting this many rows
	fs.Uint64Var(&queryLogConfigInstance.RowThreshold, "querylog-row-threshold", queryLogConfigInstance.RowThreshold, "Number of rows a query has to return or affect before being logged; not useful for streaming queries. 0 means all queries will be logged.")

	// QueryLogTimeThreshold only log queries with execution time over the time duration threshold
	fs.DurationVar(&queryLogConfigInstance.TimeThreshold, "querylog-time-threshold", queryLogConfigInstance.TimeThreshold, "Execution time duration a query needs to run over before being logged; time duration expressed in the form recognized by time.ParseDuration; not useful for streaming queries.")

	// QueryLogSampleRate causes a sample of queries to be logged
	fs.Float64Var(&queryLogConfigInstance.sampleRate, "querylog-sample-rate", queryLogConfigInstance.sampleRate, "Sample rate for logging queries. Value must be between 0.0 (no logging) and 1.0 (all queries)")

	// QueryLogMode controls the mode for logging queries (all or error)
	fs.StringVar(&queryLogConfigInstance.Mode, "querylog-mode", queryLogConfigInstance.Mode, `Mode for logging queries. "error" will only log queries that return an error. Otherwise all queries will be logged.`)
}

// StreamLogger is a non-blocking broadcaster of messages.
// Subscribers can use channels or HTTP.
type StreamLogger[T any] struct {
	name       string
	size       int
	mu         sync.Mutex
	subscribed map[chan T]string
}

// LogFormatter is the function signature used to format an arbitrary
// message for the given output writer.
type LogFormatter func(out io.Writer, params url.Values, message any) error

// New returns a new StreamLogger that can stream events to subscribers.
// The size parameter defines the channel size for the subscribers.
func New[T any](name string, size int) *StreamLogger[T] {
	return &StreamLogger[T]{
		name:       name,
		size:       size,
		subscribed: make(map[chan T]string),
	}
}

// Send sends message to all the writers subscribed to logger. Calling
// Send does not block.
func (logger *StreamLogger[T]) Send(message T) {
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
func (logger *StreamLogger[T]) Subscribe(name string) chan T {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	ch := make(chan T, logger.size)
	logger.subscribed[ch] = name
	return ch
}

// Unsubscribe removes the channel from the subscription.
func (logger *StreamLogger[T]) Unsubscribe(ch chan T) {
	logger.mu.Lock()
	defer logger.mu.Unlock()

	delete(logger.subscribed, ch)
}

// Name returns the name of StreamLogger.
func (logger *StreamLogger[T]) Name() string {
	return logger.name
}

// ServeLogs registers the URL on which messages will be broadcast.
// It is safe to register multiple URLs for the same StreamLogger.
func (logger *StreamLogger[T]) ServeLogs(url string, logf LogFormatter) {
	servenv.HTTPHandleFunc(url, func(w http.ResponseWriter, r *http.Request) {
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
			if err := logf(w, r.Form, message); err != nil {
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
func (logger *StreamLogger[T]) LogToFile(path string, logf LogFormatter) (chan T, error) {
	rotateChan := make(chan os.Signal, 1)
	setupRotate(rotateChan)

	logChan := logger.Subscribe("FileLog")
	formatParams := map[string][]string{"full": {}}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case record := <-logChan:
				logf(f, formatParams, record) // nolint:errcheck
			case <-rotateChan:
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
	Logf(io.Writer, url.Values) error
}

// GetFormatter returns a formatter function for objects conforming to the
// Formatter interface
func GetFormatter[T any](logger *StreamLogger[T]) LogFormatter {
	return func(w io.Writer, params url.Values, val any) error {
		fmter, ok := val.(Formatter)
		if !ok {
			_, err := fmt.Fprintf(w, "Error: unexpected value of type %T in %s!", val, logger.Name())
			return err
		}
		return fmter.Logf(w, params)
	}
}

// shouldSampleQuery returns true if a query should be sampled based on sampleRate
func (qlConfig QueryLogConfig) shouldSampleQuery() bool {
	if qlConfig.sampleRate <= 0 {
		return false
	} else if qlConfig.sampleRate >= 1 {
		return true
	}
	return rand.Float64() <= qlConfig.sampleRate
}

// ShouldEmitLog returns whether the log with the given SQL query
// should be emitted or filtered
func (qlConfig QueryLogConfig) ShouldEmitLog(sql string, rowsAffected, rowsReturned uint64, totalTime time.Duration, hasError bool) bool {
	if qlConfig.shouldSampleQuery() {
		return true
	}
	if qlConfig.RowThreshold > max(rowsAffected, rowsReturned) && qlConfig.FilterTag == "" {
		return false
	}
	if qlConfig.TimeThreshold > totalTime && qlConfig.TimeThreshold > 0 && qlConfig.FilterTag == "" {
		return false
	}
	if qlConfig.FilterTag != "" {
		return strings.Contains(sql, qlConfig.FilterTag)
	}
	if qlConfig.Mode == QueryLogModeError {
		return hasError
	}
	return true
}
