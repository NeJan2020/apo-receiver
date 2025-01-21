package redis

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisClient struct {
	rdb        *redis.Client
	expireTime time.Duration
}

func NewRedisClient(address string, password string, expireTime int64) (*RedisClient, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       0,
	})
	_, err := rdb.Ping(rdb.Context()).Result()
	if err != nil {
		return nil, err
	}
	if expireTime <= 0 {
		expireTime = 60
	}
	return &RedisClient{
		rdb:        rdb,
		expireTime: time.Duration(expireTime * 1000000000),
	}, nil
}

func (client *RedisClient) pubChannel(channelName string, message string) error {
	return client.rdb.Publish(context.Background(), channelName, message).Err()
}

func (client *RedisClient) subscribeChannel(channelName string, subscriber Subscriber) {
	sub := client.rdb.Subscribe(context.Background(), channelName)
	defer sub.Close()

	ch := sub.Channel()
	for msg := range ch {
		subscriber.Consume(msg.Payload)
	}
}

func (client *RedisClient) storeList(key string, data string) {
	if err := client.rdb.RPush(context.Background(), key, data).Err(); err != nil {
		log.Printf("[x Store %s] %v", key, err)
		return
	}
	if err := client.rdb.Expire(context.Background(), key, client.expireTime).Err(); err != nil {
		log.Printf("[x Expire %s] %v", key, err)
	}
}

func (client *RedisClient) getListSize(key string) int64 {
	return client.rdb.LLen(context.Background(), key).Val()
}

func (client *RedisClient) getList(key string, size int64) []string {
	result, err := client.rdb.LRange(context.Background(), key, 0, size).Result()
	if err != nil {
		log.Printf("[x GetList %s] %v", key, err)
		return nil
	}
	return result
}

func (client *RedisClient) ltrimList(key string, size int64) error {
	if _, err := client.rdb.LTrim(context.Background(), key, size, -1).Result(); err != nil {
		log.Printf("[x TrimList %s] %v", key, err)
		return err
	}
	return nil
}

func (client *RedisClient) getNo(key string) int64 {
	result, err := client.rdb.Get(context.Background(), key).Result()
	if err != nil {
		return -1
	}
	val, err := strconv.ParseInt(result, 10, 64)
	if err != nil {
		log.Printf("[x Get %s] %v", key, err)
		return -2
	}
	return val
}

func (client *RedisClient) has(key string) bool {
	result, _ := client.rdb.Exists(context.Background(), key).Result()
	return result > 0
}

func (client *RedisClient) setIntWithExpireTime(key string, value int64, expireSecond int64) {
	if err := client.rdb.Set(context.Background(), key, value, time.Duration(expireSecond)*time.Second).Err(); err != nil {
		log.Printf("[x Store %s] %v", key, err)
	}
}

func (client *RedisClient) setNxIntWithExpireTime(key string, value int64, expireSecond int64) bool {
	success, _ := client.rdb.SetNX(context.Background(), key, value, time.Duration(expireSecond)*time.Second).Result()
	return success
}

func (client *RedisClient) setInt(key string, value int64) bool {
	success, _ := client.rdb.SetNX(context.Background(), key, value, client.expireTime).Result()
	return success
}

func (client *RedisClient) getInt(key string) int64 {
	result, err := client.rdb.Get(context.Background(), key).Result()
	if err != nil {
		return 0
	}
	value, err := strconv.ParseInt(result, 10, 64)
	if err != nil {
		log.Printf("[x ParseInt %d] %v", value, err)
		return 0
	}
	return value
}

func (client *RedisClient) set(key string, value string, expireSecond int) bool {
	success, _ := client.rdb.SetNX(context.Background(), key, value, time.Duration(expireSecond)*time.Second).Result()
	return success
}

func (client *RedisClient) get(key string) string {
	result, err := client.rdb.Get(context.Background(), key).Result()
	if err != nil {
		return ""
	}
	return result
}

func (client *RedisClient) incr(key string) int64 {
	result, err := client.rdb.Incr(context.Background(), key).Result()
	if err != nil {
		log.Printf("[x Incr %s] %v", key, err)
		return -1
	}
	return result
}

func (client *RedisClient) xAddChannel(streamName string, message string) error {
	return client.rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamName,
		Values: map[string]interface{}{"message": message},
	}).Err()
}

func (client *RedisClient) xReadGroup(groupName string, consumerName string, streamName string, subscriber Subscriber) error {
	messages, err := client.rdb.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumerName,
		Streams:  []string{streamName, ">"},
		Block:    0,
		Count:    1,
		NoAck:    false,
	}).Result()
	if err != nil {
		if strings.HasSuffix(err.Error(), "connection refused") {
			time.Sleep(time.Second)
		}
		return err
	}
	for _, stream := range messages {
		for _, message := range stream.Messages {
			messageID := message.ID
			subscriber.Consume(message.Values["message"].(string))

			_, err := client.rdb.XAck(context.Background(), streamName, groupName, messageID).Result()
			if err != nil {
				log.Println("Error acknowledging message:", err)
			}
		}
	}
	return nil
}
