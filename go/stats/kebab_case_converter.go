package stats

import (
	"regexp"
	"strings"
	"sync"
)

// toKebabCase produces a monitoring compliant name from the
// original. It converts CamelCase to camel-case,
// and CAMEL_CASE to camel-case. For numbers, it
// converts 0.5 to v0_5.
func toKebabCase(name string) (hyphenated string) {
	memoizer.Lock()
	defer memoizer.Unlock()
	if hyphenated = memoizer.memo[name]; hyphenated != "" {
		return hyphenated
	}
	hyphenated = name
	for _, converter := range kebabConverters {
		hyphenated = converter.re.ReplaceAllString(hyphenated, converter.repl)
	}
	hyphenated = strings.ToLower(hyphenated)
	memoizer.memo[name] = hyphenated
	return
}

var kebabConverters = []struct {
	re   *regexp.Regexp
	repl string
}{
	// example: LC -> L-C (e.g. CamelCase -> Camel-Case).
	{regexp.MustCompile("([a-z])([A-Z])"), "$1-$2"},
	// example: CCa -> C-Ca (e.g. CCamel -> C-Camel).
	{regexp.MustCompile("([A-Z])([A-Z][a-z])"), "$1-$2"},
	{regexp.MustCompile("_"), "-"},
	{regexp.MustCompile(`\.`), "_"},
}

var memoizer = memoizerType{
	memo: make(map[string]string),
}

type memoizerType struct {
	sync.Mutex
	memo map[string]string
}
