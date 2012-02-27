package main

import target "vitess/timer"
import "testing"
import "regexp"

var tests = []testing.InternalTest{
	{"timer.TestWait", target.TestWait},
	{"timer.TestReset", target.TestReset},
	{"timer.TestIndefinite", target.TestIndefinite},
	{"timer.TestClose", target.TestClose},
}

var benchmarks = []testing.InternalBenchmark{
}
var examples = []testing.InternalExample{}

var matchPat string
var matchRe *regexp.Regexp

func matchString(pat, str string) (result bool, err error) {
	if matchRe == nil || matchPat != pat {
		matchPat = pat
		matchRe, err = regexp.Compile(matchPat)
		if err != nil {
			return
		}
	}
	return matchRe.MatchString(str), nil
}

func main() {
	testing.Main(matchString, tests, benchmarks, examples)
}
