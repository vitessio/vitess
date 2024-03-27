package redis_test

import (
	"strings"
	"testing"

	"vitess.io/vitess/go/cache/redis"
)

func Test_GenerateCacheKey_GeneratesACacheKey(t *testing.T) {
	expected := "id_user_id_4_5"
	key := redis.GenerateCacheKey("id", "user_id", "4", "5")

	if strings.Compare(expected, key) != 0 {
		t.Error()
	}
}
