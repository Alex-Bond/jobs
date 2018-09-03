package broker

import (
	"errors"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/satori/go.uuid"
	"github.com/spiral/jobs"
)

// Redis run queue using redis pool.
type Redis struct {
	sync.Mutex
	sync.WaitGroup
	namespace string
	pool      *redis.Pool
	threads   int
	queue     chan entryTest
	handler   jobs.Handler
	error     jobs.ErrorHandler
}

type entryTest struct {
	id  string
	job *jobs.Job
}

// Init configures local job broker.
func (l *Redis) Init(cfg *RedisConfig) (bool, error) {
	l.namespace = "spiral"
	l.threads = 10
	l.pool = newRedisPool(cfg.Address, l.threads, l.namespace)

	cleanNamespace(l.namespace, l.pool)

	return true, nil
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
			panic(err)
		}
	}
}

func newRedisPool(address string, threads int, namespace string) *redis.Pool {
	return &redis.Pool{
		MaxActive:   threads,
		MaxIdle:     10,
		IdleTimeout: 10 * time.Second,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", address)
			if err != nil {
				return nil, err
			}

			return conn, nil
		},
		Wait: true,
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
		l.threads = pipelines[0].Options.Integer("threads", 1)
		if l.threads < 1 {
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
	conn := l.pool.Get()
	defer conn.Close()

	for i := 0; i < l.threads; i++ {
		l.Add(1)
		go l.listen()
	}

	l.Wait()
	return nil
}

// Stop local broker.
func (l *Redis) Stop() {
	err := l.pool.Close()

	if err != nil {

	}

	l.Lock()
	defer l.Unlock()

	if l.queue != nil {
		close(l.queue)
	}
}

// Push new job to queue
func (l *Redis) Push(p *jobs.Pipeline, j *jobs.Job) (string, error) {
	conn := l.pool.Get()

	id := uuid.NewV4()

	go func() {
		l.queue <- entryTest{id: id.String(), job: j}
	}()

	return id.String(), nil
}

func (l *Redis) listen() {
	defer l.Done()
	for q := range l.queue {
		id, job := q.id, q.job

		if job.Options.Delay != 0 {
			time.Sleep(job.Options.DelayDuration())
		}

		// local broker does not support job timeouts yet
		err := l.handler(id, job)
		if err == nil {
			continue
		}

		if !job.CanRetry() {
			l.error(id, job, err)
			continue
		}

		if job.Options.RetryDelay != 0 {
			time.Sleep(job.Options.RetryDuration())
		}

		l.queue <- entryTest{id: id, job: job}
	}
}
