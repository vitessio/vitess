package tabletenv

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
