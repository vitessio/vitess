package messages

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// Add adds a task to the queue
func (q *Queue) Add(ctx context.Context, e Execer, messageID int64, data ...interface{}) error {
	// only set a random messageID if the user didn't provide one
	if messageID == 0 {
		messageID = rand.Int63()
	}

	// create default args array
	args := []interface{}{messageID}

	// append user data to args
	args = append(args, data...)

	_, err := e.ExecContext(ctx, q.insertScheduledSQL, args...)
	return err
}

// AddScheduled adds a task to the queue to be executed at the specified time
// timeScheduled needs to be in Unix Nanoseconds
func (q *Queue) AddScheduled(ctx context.Context, e Execer, messageID, timeScheduled int64, data ...interface{}) error {
	// only set a random messageID if the user didn't provide one
	if messageID == 0 {
		messageID = rand.Int63()
	}

	if timeScheduled == 0 {
		timeScheduled = time.Now().UTC().UnixNano()
	}

	// create default args array
	args := []interface{}{timeScheduled, messageID}

	// append user data to args
	args = append(args, data...)

	_, err := e.ExecContext(ctx, q.insertSQL, args...)
	return err
}

// Get returns the next available message. It blocks until either a message
// is available or the context is cancelled.
func (q *Queue) Get(ctx context.Context, dest ...interface{}) error {
	q.s.mu.RLock()
	defer q.s.mu.RUnlock()

	if !q.s.isOpen {
		return errors.New("cannot perform Get on closed queue")
	}

	select {
	// send the scan targets through the channel - this will block until rows.Next()
	// has another row available
	case q.s.destChan <- dest:
		// block and wait for the scan error to be returned
		return <-q.s.errChan

	case <-ctx.Done():
		return ctx.Err()
	}
}

// Ack marks a message as successfully completed
func (q *Queue) Ack(ctx context.Context, e Execer, messageID int64) error {
	_, err := e.ExecContext(ctx, q.s.ackSQL, time.Now().UTC().UnixNano(), messageID)
	return err
}

// Nack marks a message as unsuccessfully completed
func (q *Queue) Nack(ctx context.Context, e Execer, messageID int64) error {
	// TODO: Add api to message manager to immediately nack
	// calling this now has no effect, and the message is requeued after timing out
	return nil
}

// Fail marks a task as failed, and it will not be queued again until manual action is taken
func (q *Queue) Fail(ctx context.Context, e Execer, messageID int64) error {
	_, err := e.ExecContext(ctx, q.s.failSQL, messageID)
	return err
}
