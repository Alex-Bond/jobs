package sqs

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/spiral/jobs"
	"sync"
)

// Broker run jobs using Broker service.
type Broker struct {
	cfg         *Config
	mu          sync.Mutex
	stop        chan interface{}
	sqs         *sqs.SQS
	wg          sync.WaitGroup
	queue       map[*jobs.Pipeline]*Queue
	handlerPool chan jobs.Handler
	err         jobs.ErrorHandler
}

// Listen configures broker with list of tubes to listen and handler function. Local broker groups all tubes
// together.
func (b *Broker) Listen(pipelines []*jobs.Pipeline, pool chan jobs.Handler, err jobs.ErrorHandler) error {
	b.queue = make(map[*jobs.Pipeline]*Queue)
	for _, p := range pipelines {
		if err := b.registerQueue(p); err != nil {
			return err
		}
	}

	b.handlerPool = pool
	b.err = err
	return nil
}

// Init configures local job broker.
func (b *Broker) Init(cfg *Config) (bool, error) {
	b.cfg = cfg
	return true, nil
}

// Serve tubes.
func (b *Broker) Serve() (err error) {
	b.sqs, err = b.cfg.SQS()
	if err != nil {
		return err
	}

	b.mu.Lock()
	b.stop = make(chan interface{})
	b.mu.Unlock()

	for _, q := range b.queue {
		if q.Create {
			if err := b.createQueue(q); err != nil {
				return err
			}
		}

		url, err := b.sqs.GetQueueUrl(&sqs.GetQueueUrlInput{
			QueueName: aws.String(q.Queue),
		})

		if err != nil {
			return err
		}

		q.URL = url.QueueUrl

		if q.Listen {
			b.wg.Add(1)
			go b.listen(q)
		}
	}

	b.wg.Wait()
	<-b.stop

	return nil
}

// Stop serving.
func (b *Broker) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stop != nil {
		close(b.stop)
	}
}

// Push new job to queue
func (b *Broker) Push(p *jobs.Pipeline, j *jobs.Job) (string, error) {
	data, err := json.Marshal(j)
	if err != nil {
		return "", err
	}

	result, err := b.sqs.SendMessage(&sqs.SendMessageInput{
		DelaySeconds: aws.Int64(int64(j.Options.Delay)),
		MessageBody:  aws.String(string(data)),
		QueueUrl:     b.queue[p].URL,
	})

	if err != nil {
		return "", err
	}

	return *result.MessageId, nil
}

// registerTube new beanstalk pipeline
func (b *Broker) registerQueue(pipeline *jobs.Pipeline) error {
	queue, err := NewQueue(pipeline)
	if err != nil {
		return err
	}

	b.queue[pipeline] = queue
	return nil
}

// createQueue creates sqs queue.
func (b *Broker) createQueue(q *Queue) error {
	_, err := b.sqs.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  aws.String(q.Queue),
		Attributes: q.CreateAttributes(),
	})

	return err
}

// listen jobs from given tube
func (b *Broker) listen(q *Queue) {
	defer b.wg.Done()
	var job *jobs.Job
	var handler jobs.Handler
	for {
		select {
		case <-b.stop:
			return
		default:
			result, err := b.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueUrl:            q.URL,
				MaxNumberOfMessages: aws.Int64(1),
				VisibilityTimeout:   aws.Int64(int64(q.Timeout)),
				WaitTimeSeconds:     aws.Int64(int64(q.WaitTime)),
			})

			if err != nil {
				// need additional logging
				continue
			}

			if len(result.Messages) == 0 {
				continue
			}

			err = json.Unmarshal([]byte(*result.Messages[0].Body), &job)
			if err != nil {
				// need additional logging
				continue
			}

			handler = <-b.handlerPool
			go func() {
				jerr := handler(*result.Messages[0].MessageId, job)
				b.handlerPool <- handler

				if jerr == nil {
					b.sqs.DeleteMessage(&sqs.DeleteMessageInput{
						QueueUrl: q.URL, ReceiptHandle: result.Messages[0].ReceiptHandle,
					})
					return
				}

				if !job.CanRetry() {
					b.sqs.DeleteMessage(&sqs.DeleteMessageInput{
						QueueUrl: q.URL, ReceiptHandle: result.Messages[0].ReceiptHandle,
					})

					b.err(*result.Messages[0].MessageId, job, jerr)
					return
				}

				data, err := json.Marshal(job)
				if err != nil {
					return
				}

				b.sqs.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl: q.URL, ReceiptHandle: result.Messages[0].ReceiptHandle,
				})

				// retry job
				b.sqs.SendMessage(&sqs.SendMessageInput{
					DelaySeconds: aws.Int64(int64(job.Options.RetryDelay)),
					MessageBody:  aws.String(string(data)),
					QueueUrl:     q.URL,
				})
			}()
		}
	}
}
