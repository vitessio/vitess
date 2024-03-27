package redis

import "strings"

func GenerateCacheKey(args ...string) string {
	return strings.Join(args, "_")
}
