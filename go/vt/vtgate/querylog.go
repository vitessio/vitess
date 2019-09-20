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
	"flag"
	"net/http"

	"vitess.io/vitess/go/streamlog"
)

var (
	// QueryLogHandler is the debug UI path for exposing query logs
	QueryLogHandler = "/debug/querylog"

	// QueryLogzHandler is the debug UI path for exposing query logs
	QueryLogzHandler = "/debug/querylogz"

	// QueryzHandler is the debug UI path for exposing query plan stats
	QueryzHandler = "/debug/queryz"

	// QueryLogger enables streaming logging of queries
	QueryLogger = streamlog.New("VTGate", 10)

	// queryLogToFile controls whether query logs are sent to a file
	queryLogToFile = flag.String("log_queries_to_file", "", "Enable query logging to the specified file")
)

func initQueryLogger(vtg *VTGate) error {
	QueryLogger.ServeLogs(QueryLogHandler, streamlog.GetFormatter(QueryLogger))

	http.HandleFunc(QueryLogzHandler, func(w http.ResponseWriter, r *http.Request) {
		ch := QueryLogger.Subscribe("querylogz")
		defer QueryLogger.Unsubscribe(ch)
		querylogzHandler(ch, w, r)
	})

	http.HandleFunc(QueryzHandler, func(w http.ResponseWriter, r *http.Request) {
		queryzHandler(vtg.executor, w, r)
	})

	if *queryLogToFile != "" {
		_, err := QueryLogger.LogToFile(*queryLogToFile, streamlog.GetFormatter(QueryLogger))
		if err != nil {
			return err
		}
	}

	return nil
}
