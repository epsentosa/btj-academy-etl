package lib

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	Client *redis.Client
}

type InputFile struct {
	File []byte
}

func NewRedisClient(ctx context.Context, host string, db int) (*RedisClient, error) {
	redisHost := fmt.Sprintf("%s:6379", host)
	client := redis.NewClient(&redis.Options{
		Addr: redisHost,
		DB:   db,
	})
	err := client.Ping(ctx).Err()
	if err != nil {
		return nil, errors.Wrap(err, "failed connecting to redis")
	}
	return &RedisClient{
		Client: client,
	}, nil
}

func (r *RedisClient) GetInputFile(ctx context.Context, key string) (*InputFile, error) {
	file, err := r.Client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, errors.Errorf("file %v doesn't exist in redis", key)
	} else if err != nil {
		return nil, errors.Errorf("failed to get content %v from redis", key)
	}

	return &InputFile{
		File: file,
	}, nil
}

func (r *RedisClient) DeleteInputFile(ctx context.Context, key string) error {
	return r.Client.Del(ctx, key).Err()
}
