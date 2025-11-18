package tabletenv

// QueryExecutionHook is used to run arbitrary data processing on logStats
// RunQueryExecutionHooks is run in query_executor.go/Execute
// See mockQueryExecutionHook in query_executor_test.go for a very simple example
type QueryExecutionHook interface {
	OnQueryCompleted(logStats *LogStats)
}

var queryExecutionHooks = []QueryExecutionHook{}

func RegisterQueryExecutionHook(hook QueryExecutionHook) {
	queryExecutionHooks = append(queryExecutionHooks, hook)
}

func RunQueryExecutionHooks(logStats *LogStats) {
	for _, hook := range queryExecutionHooks {
		hook.OnQueryCompleted(logStats)
	}
}
