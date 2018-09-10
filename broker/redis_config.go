package broker

import (
	"errors"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/spiral/jobs"
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
	IdleTimeout int
}

// RedisPipeline is redis specific pipeline
type redisPipeline struct {
	Listen  bool
	Queue   string
	Mode    string
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
		"fifo",
		"lifo",
		"broadcast",
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

func createPipline(p *jobs.Pipeline) (*redisPipeline, error) {
	rp := &redisPipeline{
		Listen:  p.Listen,
		Queue:   p.Options.String("queue", ""),
		Mode:    p.Options.String("mode", fifo.String()),
		Timeout: p.Options.Integer("timeout", 10),
	}
	if err := rp.checkConfig(); err != nil {
		return nil, err
	}

	return rp, nil
}

func (p *redisPipeline) checkConfig() error {
	if p.Queue == "" {
		return errors.New("missing or incorrect queue for redis pipeline")
	}

	if p.Mode == "Unknown" {

	}

	return nil
}

func (c *redisConfig) Conn(address string, threads int, namespace string) *redis.Pool {
	return &redis.Pool{
		MaxActive:   c.Threads,
		MaxIdle:     c.MaxIdleConnections,
		// TODO rebuild
		IdleTimeout:  time.Second * 20,
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
