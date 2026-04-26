package client

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config holds Redis connection settings.
type Config struct {
	Addr     string
	Password string
	DB       int
}

// Client wraps go-redis with the operations needed by the logging system.
type Client struct {
	rdb *redis.Client
}

// New creates a Client. Call Ping to verify connectivity.
func New(cfg Config) *Client {
	return &Client{
		rdb: redis.NewClient(&redis.Options{
			Addr:     cfg.Addr,
			Password: cfg.Password,
			DB:       cfg.DB,
		}),
	}
}

// Ping verifies the connection to Redis.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// SetNX sets key with a TTL only when the key does not already exist.
// Returns true if the key was newly created (i.e. the log entry is NOT a duplicate).
// Returns false if the key already existed (duplicate detected).
func (c *Client) SetNX(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	ok, err := c.rdb.SetNX(ctx, key, 1, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redis setnx %s: %w", key, err)
	}
	return ok, nil
}

// IncrWithTTL atomically increments an integer counter and sets its expiry on
// first creation. Useful for rate-limit and alert window counters.
// Returns the new counter value.
func (c *Client) IncrWithTTL(ctx context.Context, key string, ttl time.Duration) (int64, error) {
	pipe := c.rdb.TxPipeline()
	incrCmd := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("redis incr %s: %w", key, err)
	}
	return incrCmd.Val(), nil
}

// Get retrieves the string value of a key. Returns ("", nil) when the key is absent.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	val, err := c.rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("redis get %s: %w", key, err)
	}
	return val, nil
}

// Del deletes one or more keys.
func (c *Client) Del(ctx context.Context, keys ...string) error {
	return c.rdb.Del(ctx, keys...).Err()
}

// Close closes the Redis connection.
func (c *Client) Close() error {
	return c.rdb.Close()
}
