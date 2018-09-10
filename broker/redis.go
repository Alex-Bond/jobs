package broker

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"sync"
	"time"
	"unsafe"

	"github.com/gomodule/redigo/redis"
	"github.com/satori/go.uuid"
	"github.com/spiral/jobs"
)

// Redis run queue using redis pool.
type Redis struct {
	sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	cfg       *redisConfig
	pipelines map[*jobs.Pipeline]*redisPipeline
	pool      *redis.Pool
	handler   jobs.Handler
	error     jobs.ErrorHandler
}

// Init configures local job broker.
func (l *Redis) Init(cfg *redisConfig) (bool, error) {
	if !cfg.Enable {
		return false, nil
	}
	l.cfg = cfg
	l.ctx, l.cancel = context.WithCancel(context.Background())

	cleanNamespace(l.cfg.Namespace, l.pool)
	//l.createQueues(l.cfg., l.cfg)

	return true, nil
}

func (l *Redis) createQueues(queueNumber int, config *redisConfig) {
	for i := 0; i < queueNumber; i++ {
		//config.
	}
}

func cleanNamespace(namespace string, pool *redis.Pool) {
	conn := pool.Get()
	defer conn.Close()

	allKeysInNamespace := namespace + "*"

	keys, err := redis.Strings(conn.Do("KEYS", allKeysInNamespace))
	if err != nil {
		// TODO create error message
		panic(err)
	}

	for i := 0; i < len(keys); i++ {
		if _, err := conn.Do("DEL", keys[i]); err != nil {
			// TODO create error message
			panic("could not delete:" + err.Error())
		}
	}
}

// Handle configures broker with list of pipelines to listen and handler function. Redis broker groups all pipelines
// together.
func (l *Redis) Handle(pipelines []*jobs.Pipeline, h jobs.Handler, f jobs.ErrorHandler) error {
	switch {
	case len(pipelines) == 0:
		// no pipelines to handled
		return nil

	case len(pipelines) == 1:
		l.cfg.Threads = pipelines[0].Options.Integer("threads", 1)
		if l.cfg.Threads < 1 {
			return errors.New("local queue `thread` number must be 1 or higher")
		}

	default:
		return errors.New("local queue handler expects exactly one pipeline")
	}

	l.handler = h
	l.error = f
	return nil
}

// Serve local broker.
func (l *Redis) Serve() error {
	for _, p := range l.pipelines {
		// Get connection from the redis pool
		conn := l.pool.Get()
		// Count wg
		l.Add(1)
		// Pass connection to the goroutine
		go func(c redis.Conn, pp *redisPipeline) {
			if p.Listen {
				defer func() {
					c.Close()
				}()

				// Blocks until wg.Done
				l.listen(pp)
			}
		}(conn, p)
	}

	l.Wait()
	return nil
}

// Stop local broker.
func (l *Redis) Stop() {
	l.pool.Close()
	// Raise context cancel
	l.cancel()
}

// Push new job to queue
func (l *Redis) Push(p *jobs.Pipeline, j *jobs.Job) (string, error) {
	// Get connection from the redis pool
	conn := l.pool.Get()

	// Generate new key
	id := uuid.NewV4()

	// Serialize job
	b, err := j.Serialize()
	if err != nil {
		return "", err
	}

	switch l.pipelines[p].Mode {
	case fifo.String():
		conn.Do("LPUSH", redisGeneralNamespace(l.pipelines[p].Queue), b)
	case lifo.String():
		conn.Do("RPUSH", redisGeneralNamespace(l.pipelines[p].Queue), b)
	case broadcast.String():
		for _, v := range l.pipelines {
			go func(rp *redisPipeline, serJob []byte) {
				conn.Do("LPUSH", redisGeneralNamespace(l.pipelines[p].Queue), serJob)
			}(v, b)
		}
	}

	// return key
	return id.String(), nil
}

func (l *Redis) listen(p *redisPipeline) error {
	for {
		select {
		case <-l.ctx.Done():
			return errors.New("context done")

		default:
			conn := l.pool.Get()
			reply, err := conn.Do("BLPOP", time.Second*10, redisGeneralNamespace(p.Queue))
			if err != nil {
				return err
			}

			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			err = enc.Encode(reply)
			if err != nil {
				return err
			}

			j := jobs.Job{}
			if err := json.Unmarshal(buf.Bytes(), &j); err != nil {
				return err
			}
			l.handler("test", &j)
		}
	}
}

// FromInterfaceToBytes
//Benchmark_Fun-8   	2000000000	         0.63 ns/op	       0 B/op	       0 allocs/op
func FromInterfaceToBytes(intr interface{}) []byte {
	p := *(*interface{})(unsafe.Pointer(&intr))
	b := p.([]byte)
	return b
}
