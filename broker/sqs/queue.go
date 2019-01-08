package sqs

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/spiral/jobs"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type queue struct {
	active int32
	pipe   *jobs.Pipeline
	sqs    *sqs.SQS
	url    *string

	// durations
	reserve    time.Duration
	cmdTimeout time.Duration

	// tube events
	lsn func(event int, ctx interface{})

	// stop channel
	wait chan interface{}

	// active operations
	muw sync.RWMutex
	wg  sync.WaitGroup

	// exec handlers
	fetchPool chan interface{}
	execPool  chan jobs.Handler
	err       jobs.ErrorHandler
}

func newQueue(
	pipe *jobs.Pipeline,
	sqs *sqs.SQS,
	url *string,
	reserve time.Duration,
	lsn func(event int, ctx interface{}),
) (*queue, error) {
	return &queue{
		pipe:    pipe,
		sqs:     sqs,
		url:     url,
		reserve: reserve,
		lsn:     lsn,
	}, nil
}

// associate queue with new consume pool
func (q *queue) configure(execPool chan jobs.Handler, err jobs.ErrorHandler) error {
	q.execPool = execPool
	q.err = err

	return nil
}

// serve consumers
func (q *queue) serve(prefetch int) {
	q.wait = make(chan interface{})
	q.fetchPool = make(chan interface{}, prefetch)
	atomic.StoreInt32(&q.active, 1)

	for i := 0; i < prefetch; i++ {
		q.fetchPool <- nil
	}

	for {
		msg, job, eof := q.fetchMessage()
		if eof {
			return
		}

		if msg == nil {
			continue
		}

		go func(msg *sqs.Message) {
			err := q.consume(<-q.execPool, msg, job)
			q.fetchPool <- nil
			q.wg.Done()

			if err != nil {
				q.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: q.pipe, Caused: err})
			}
		}(msg)
	}
}

// fetchMessage and allocate connection. fetchPool must be refilled manually.
func (q *queue) fetchMessage() (msg *sqs.Message, job *jobs.Job, eof bool) {
	q.muw.Lock()
	defer q.muw.Unlock()

	select {
	case <-q.wait:
		return nil, nil, true
	case <-q.fetchPool:
		result, err := q.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            q.url,
			MaxNumberOfMessages: aws.Int64(1),
			WaitTimeSeconds:     aws.Int64(int64(q.reserve.Seconds())),
			AttributeNames:      []*string{aws.String("ApproximateReceiveCount")},
		})

		if err != nil {
			q.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: q.pipe, Caused: err})
			q.fetchPool <- nil
			return nil, nil, false
		}

		if len(result.Messages) == 0 {
			q.fetchPool <- nil
			return nil, nil, false
		}

		q.wg.Add(1)

		job := &jobs.Job{}
		err = json.Unmarshal([]byte(*result.Messages[0].Body), &job)
		if err != nil {
			q.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: q.pipe, Caused: err})
			q.fetchPool <- nil
			return nil, nil, false
		}

		return result.Messages[0], job, false
	}
}

// consume single message
func (q *queue) consume(h jobs.Handler, msg *sqs.Message, j *jobs.Job) (err error) {
	// block the job
	_, err = q.sqs.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueUrl:          q.url,
		ReceiptHandle:     msg.ReceiptHandle,
		VisibilityTimeout: aws.Int64(int64(j.Options.TimeoutDuration().Seconds())),
	})

	if err != nil {
		return err
	}

	err = h(*msg.MessageId, j)
	q.execPool <- h

	if err == nil {
		// success
		_, err = q.sqs.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      q.url,
			ReceiptHandle: msg.ReceiptHandle,
		})

		return err
	}

	// failed
	q.err(*msg.MessageId, j, err)

	reserves, _ := strconv.Atoi(*msg.Attributes["ApproximateReceiveCount"])
	if reserves != 0 && j.Options.CanRetry(reserves) {
		// retry after specified duration
		_, err = q.sqs.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
			QueueUrl:          q.url,
			ReceiptHandle:     msg.ReceiptHandle,
			VisibilityTimeout: aws.Int64(int64(j.Options.RetryDelay)),
		})

		return err
	}

	return nil
}

// stop the queue consuming
func (q *queue) stop() {
	if atomic.LoadInt32(&q.active) == 0 {
		return
	}

	atomic.StoreInt32(&q.active, 0)

	close(q.wait)
	q.muw.Lock()
	q.wg.Wait() // wait for all the jobs to complete
	q.muw.Unlock()
}

// add job to the queue
func (q *queue) send(data []byte, delay, retry time.Duration) (string, error) {
	result, err := q.sqs.SendMessage(&sqs.SendMessageInput{
		DelaySeconds: aws.Int64(int64(delay.Seconds())),
		MessageBody:  aws.String(string(data)),
		QueueUrl:     q.url,
	})

	if err != nil {
		return "", err
	}

	return *result.MessageId, nil
}

// return queue stats
func (q *queue) stat() (stat *jobs.Stat, err error) {
	r, err := q.sqs.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl: q.url,
		AttributeNames: []*string{
			aws.String("ApproximateNumberOfMessages"),
			aws.String("ApproximateNumberOfMessagesDelayed"),
			aws.String("ApproximateNumberOfMessagesNotVisible"),
		},
	})

	stat = &jobs.Stat{InternalName: q.pipe.String("queue", "")}

	for a, v := range r.Attributes {
		if a == "ApproximateNumberOfMessages" {
			if v, err := strconv.Atoi(*v); err == nil {
				stat.Queue = int64(v)
			}
		}

		if a == "ApproximateNumberOfMessagesNotVisible" {
			if v, err := strconv.Atoi(*v); err == nil {
				stat.Active = int64(v)
			}
		}

		if a == "ApproximateNumberOfMessagesDelayed" {
			if v, err := strconv.Atoi(*v); err == nil {
				stat.Delayed = int64(v)
			}
		}
	}

	return stat, nil
}

// throw handles service, server and pool events.
func (q *queue) throw(event int, ctx interface{}) {
	q.lsn(event, ctx)
}