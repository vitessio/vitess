package tabletenv

type QueryExecutionHook interface {
	OnQueryCompleted(logStats *LogStats)
}

var queryExecutionHook QueryExecutionHook = nil

func SetQueryExecutionHook(hook QueryExecutionHook) {
	queryExecutionHook = hook
}

func ProcessQueryCompleted(logStats *LogStats) {
	if queryExecutionHook != nil {
		queryExecutionHook.OnQueryCompleted(logStats)
	}
}
