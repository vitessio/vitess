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
