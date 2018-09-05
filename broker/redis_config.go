package broker

import (
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/spiral/roadrunner/service"
)

// RedisConfig defines connection options to Redis server.
type redisConfig struct {
	// Enable to disable service.
	Enable bool

	// Address of beanstalk server.
	Address string

	// Max active connections to pool
	Threads int

	// MaxIdleConnections in redis pool
	MaxIdleConnections int

	// Namespace for redis pool
	Namespace string

	// Close connection after remaining idle for this duration
	IdleTimeout time.Duration
}

// RedisPipeline is redis specific pipeline
type redisPipeline struct {
	Listen  bool
	Queue   string
	Mode    Mode
	Timeout int
}

// Modes for redis
type Mode int

const (
	fifo      Mode = 1
	lifo      Mode = 2
	broadcast Mode = 3
)

// String is used to convert modes from int to string names
func (m Mode) String() string {
	modes := [...]string{
		"Fifo",
		"Lifo",
		"Broadcast",
	}

	if m < fifo || m > broadcast {
		return "Unknown"
	}

	// return stringer for modes
	return modes[m]
}

// Hydrate populates config with values.
func (c *redisConfig) Hydrate(cfg service.Config) error {
	return cfg.Unmarshal(&c)
}

// Listener creates new rpc socket Listener.
//func (c *RedisConfig) Conn() (*beanstalk.Conn, error) {
//	dsn := strings.Split(c.Address, "://")
//	if len(dsn) != 2 {
//		return nil, errors.New("invalid socket DSN (tcp://:6001, unix://rpc.sock)")
//	}
//
//	if dsn[0] == "unix" {
//		syscall.Unlink(dsn[1])
//	}
//
//	return beanstalk.Dial(dsn[0], dsn[1])
//}

func (c *redisConfig) Conn(address string, threads int, namespace string) *redis.Pool {
	return &redis.Pool{
		MaxActive:   c.Threads,
		MaxIdle:     c.MaxIdleConnections,
		IdleTimeout: c.IdleTimeout,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", c.Address)
			if err != nil {
				return nil, err
			}

			return conn, nil
		},
		Wait: true,
	}
}
