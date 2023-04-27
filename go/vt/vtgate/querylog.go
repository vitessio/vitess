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

package vtgate

import (
	"net/http"
	"sync"

	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtgate/logstats"
)

var (
	// QueryLogHandler is the debug UI path for exposing query logs
	QueryLogHandler = "/debug/querylog"

	// QueryLogzHandler is the debug UI path for exposing query logs
	QueryLogzHandler = "/debug/querylogz"

	// QueryzHandler is the debug UI path for exposing query plan stats
	QueryzHandler = "/debug/queryz"

	// QueryLogger enables streaming logging of queries
	QueryLogger   *streamlog.StreamLogger[*logstats.LogStats]
	queryLoggerMu sync.Mutex
)

func SetQueryLogger(logger *streamlog.StreamLogger[*logstats.LogStats]) {
	queryLoggerMu.Lock()
	defer queryLoggerMu.Unlock()
	QueryLogger = logger
}

func initQueryLogger(vtg *VTGate) error {
	SetQueryLogger(streamlog.New[*logstats.LogStats]("VTGate", queryLogBufferSize))
	QueryLogger.ServeLogs(QueryLogHandler, streamlog.GetFormatter(QueryLogger))

	servenv.HTTPHandleFunc(QueryLogzHandler, func(w http.ResponseWriter, r *http.Request) {
		ch := QueryLogger.Subscribe("querylogz")
		defer QueryLogger.Unsubscribe(ch)
		querylogzHandler(ch, w, r)
	})

	servenv.HTTPHandleFunc(QueryzHandler, func(w http.ResponseWriter, r *http.Request) {
		queryzHandler(vtg.executor, w, r)
	})

	if queryLogToFile != "" {
		_, err := QueryLogger.LogToFile(queryLogToFile, streamlog.GetFormatter(QueryLogger))
		if err != nil {
			return err
		}
	}

	return nil
}
