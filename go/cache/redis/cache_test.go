package redis_test

import (
	"testing"

	"vitess.io/vitess/go/cache/redis"
)

func Test_RedisStringSet(t *testing.T) {
	t.Skip("E2E not set up")

	cache := redis.NewCache()

	if err := cache.Set("hi", "mom"); err != nil {
		t.Error(err)
	}

	val, err := cache.Get("hi")
	if err != nil {
		t.Error(err)
	}

	if val != "mom" {
		t.Error("Value is not \"mom\"")
	}
}

func Test_RedisStringSetAndDel(t *testing.T) {
	t.Skip("E2E not set up")

	cache := redis.NewCache()

	if err := cache.Set("hi", "mom"); err != nil {
		t.Error(err)
	}

	val, err := cache.Get("hi")
	if err != nil || len(val) == 0 {
		t.Error(err)
	}

	if err = cache.Delete("hi"); err != nil {
		t.Error(err)
	}

	val_ex, err := cache.Get("hi")
	if err == nil || len(val_ex) != 0 {
		t.Error(err)
	}
}

func Test_RedisOperationsWithKeyGen(t *testing.T) {
	t.Skip("E2E not set up")

	cache := redis.NewCache()

	cols := []string{"id", "user_id"}
	vtgs := []string{"1323", "4362"}

	key := redis.GenerateCacheKey(append(cols, vtgs...)...)

	if err := cache.Set(key, "mom"); err != nil {
		t.Error(err)
	}

	val, err := cache.Get(key)
	if err != nil || len(val) == 0 {
		t.Error(err)
	}

	if err = cache.Delete(key); err != nil {
		t.Error(err)
	}
}
