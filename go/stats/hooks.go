package stats

type statsdHook struct {
	timerHook     func(string, string, int64, *Timings)
	histogramHook func(string, int64)
}

var defaultStatsdHook = statsdHook{}

// RegisterTimerHook registers timer hook
func RegisterTimerHook(hook func(string, string, int64, *Timings)) {
	defaultStatsdHook.timerHook = hook
}

// RegisterHistogramHook registers timer hook
func RegisterHistogramHook(hook func(string, int64)) {
	defaultStatsdHook.histogramHook = hook
}
