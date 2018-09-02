package broker

import (
	"errors"
	"sync"
	"time"

	"github.com/satori/go.uuid"
	"github.com/spiral/jobs"
)

// RedisTestCopuFromLocalPhpTalkwithANton run queue using local goroutines.
type RedisTestCopuFromLocalPhpTalkwithANton struct {
	mu      sync.Mutex
	threads int
	wg      sync.WaitGroup
	queue   chan entryTest
	exec    jobs.Handler
	error   jobs.ErrorHandler
}

type entryTest struct {
	id  string
	job *jobs.Job
}

// Init configures local job broker.
func (l *RedisTestCopuFromLocalPhpTalkwithANton) Init() (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.queue = make(chan entryTest)

	return true, nil
}

// Handle configures broker with list of pipelines to listen and handler function. RedisTestCopuFromLocalPhpTalkwithANton broker groups all pipelines
// together.
func (l *RedisTestCopuFromLocalPhpTalkwithANton) Handle(pipelines []*jobs.Pipeline, h jobs.Handler, f jobs.ErrorHandler) error {

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

	l.exec = h
	l.error = f
	return nil
}

// Serve local broker.
func (l *RedisTestCopuFromLocalPhpTalkwithANton) Serve() error {
	for i := 0; i < l.threads; i++ {
		l.wg.Add(1)
		go l.listen()
	}

	l.wg.Wait()
	return nil
}

// Stop local broker.
func (l *RedisTestCopuFromLocalPhpTalkwithANton) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.queue != nil {
		close(l.queue)
	}
}

// Push new job to queue
func (l *RedisTestCopuFromLocalPhpTalkwithANton) Push(p *jobs.Pipeline, j *jobs.Job) (string, error) {
	id := uuid.NewV4()

	go func() { l.queue <- entryTest{id: id.String(), job: j} }()

	return id.String(), nil
}

func (l *RedisTestCopuFromLocalPhpTalkwithANton) listen() {
	defer l.wg.Done()
	for q := range l.queue {
		id, job := q.id, q.job

		if job.Options.Delay != 0 {
			time.Sleep(job.Options.DelayDuration())
		}

		// local broker does not support job timeouts yet
		err := l.exec(id, job)
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
