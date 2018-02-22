package messages

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// A Message stores information about a message
type Message struct {
	s *subscription

	timeScheduled int64
	ID            int64
	Data          []interface{}

	// preconfigured scan targets that point at timeScheduled, ID and the expanded elements of the Data slice
	// e.g. []interface{&timeScheduled, &ID, &Data...}
	dataPointers []interface{}

	// err is only set if there is a scan error in Subscribe
	err error
}

// AddMessage adds a task to the queue
func (q *Queue) AddMessage(ctx context.Context, e Execer, id int64, data ...interface{}) error {
	// only set a random id if the user didn't provide an id option
	if id == 0 {
		id = rand.Int63()
	}

	// create default args array
	args := []interface{}{id}

	// append user data to args
	args = append(args, data...)

	_, err := e.ExecContext(ctx, q.insertScheduledSQL, args...)
	return err
}

// AddScheduledMessage adds a task to the queue to be executed at the specified time
// timeScheduled needs to be in Unix Nanoseconds
func (q *Queue) AddScheduledMessage(ctx context.Context, e Execer, id, timeScheduled int64, data ...interface{}) error {
	// only set a random id if the user didn't provide an id option
	if id == 0 {
		id = rand.Int63()
	}

	if timeScheduled == 0 {
		timeScheduled = time.Now().UTC().UnixNano()
	}

	// create default args array
	args := []interface{}{timeScheduled, id}

	// append user data to args
	args = append(args, data...)

	_, err := e.ExecContext(ctx, q.insertSQL, args...)
	return err
}

// GetMessage returns the next available message. It blocks until either a message
// is available or the context is cancelled.
func (q *Queue) GetMessage(ctx context.Context) (*Message, error) {
	q.s.mu.RLock()
	defer q.s.mu.RUnlock()

	if !q.s.isOpen {
		return nil, errors.New("cannot perform Get on closed queue")
	}

	select {
	case m := <-q.s.readyForProcessingChan:
		if m.err != nil {
			return nil, m.err
		}
		return m, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Ack marks a message as successfully completed
func (m *Message) Ack(ctx context.Context, e Execer) error {
	defer m.close()
	_, err := e.ExecContext(ctx, m.s.ackSQL, time.Now().UTC().UnixNano(), m.timeScheduled, m.ID)
	return err
}

// Fail marks a task as failed, and it will not be queued again until manual action is taken
func (m *Message) Fail(ctx context.Context, e Execer) error {
	defer m.close()
	_, err := e.ExecContext(ctx, m.s.failSQL, m.timeScheduled, m.ID)
	return err
}

// close pushes a message back into the queue for resue
func (m *Message) close() {
	m.s.waitingForDataChan <- m
}
