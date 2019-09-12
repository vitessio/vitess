package sqlparser

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	re = regexp.MustCompile(`([^\\]?|[\\]{2})[%_]`)
)

func replacer(s string) string {
	if strings.HasPrefix(s, `\\`) {
		return s[2:]
	}

	result := strings.Replace(s, "%" ,".*", -1)
	result = strings.Replace(result, "_", ".", -1)

	return result
}

// LikeToRegexp converts a like sql expression to regular expression
func LikeToRegexp(likeExpr string) *regexp.Regexp {
	if likeExpr == "" {
		return regexp.MustCompile("^.*$") // Can never fail
	}

	keyPattern := regexp.QuoteMeta(likeExpr)
	keyPattern = re.ReplaceAllStringFunc(keyPattern, replacer)
	keyPattern = fmt.Sprintf("^%s$", keyPattern)
	return regexp.MustCompile(keyPattern) // Can never fail
}
