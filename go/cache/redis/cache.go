package redis

import (
	"time"

	gredis "github.com/redis/go-redis/v9"
	"golang.org/x/net/context"
)

const defaultTimeout = 15 * time.Second
const defaultRecordTtl = 5 * time.Minute

type Cache struct {
	client *gredis.Client
}

func NewCache() *Cache {
	opts := &gredis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	}

	client := gredis.NewClient(opts)

	return &Cache{
		client: client,
	}
}

func (c *Cache) Get(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	return c.client.Get(ctx, key).Result()
}

func (c *Cache) Set(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	return c.client.Set(ctx, key, value, defaultRecordTtl).Err()
}

func (c *Cache) Delete(key ...string) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()

	return c.client.Del(ctx, key...).Err()
}
