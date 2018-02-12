package messages

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

// An AddOption allows for manipulating a message before adding it
type AddOption func(m *Message)

// TimeScheduled sets the time the message will first be queued
func TimeScheduled(timeScheduled int64) AddOption {
	return func(m *Message) {
		m.TimeScheduled = timeScheduled
	}
}

// ID sets the message id - if not set, defaults to random id
func ID(id int64) AddOption {
	return func(m *Message) {
		m.ID = id
	}
}

// Epoch sets the message epoch
func Epoch(epoch int64) AddOption {
	return func(m *Message) {
		m.Epoch = epoch
	}
}

// CustomFieldData lets the user provide custom data
func CustomFieldData(customFieldData ...interface{}) AddOption {
	return func(m *Message) {
		m.customFieldData = customFieldData
	}
}

// AddMessage adds a task to the queue
func (q *Queue) AddMessage(ctx context.Context, conn Queryer, opts ...AddOption) error {
	// get a message with defaults from the queue sync.Pool and put it back when done
	m := q.getMessageForAdd()
	defer q.putMessageForAdd(m)

	// execute all the AddOption funcs
	for _, opt := range opts {
		opt(m)
	}

	// only set a random id if the user didn't provide an id option
	if m.ID == 0 {
		m.ID = rand.Int63()
	}

	// create default args array
	args := []interface{}{
		m.TimeScheduled,
		m.ID,
		m.Epoch,
	}

	// if the queue is defined with custom user fields, make sure that they were supplied
	if len(q.userFieldNames) != len(m.customFieldData) {
		return errors.New("custom field data does not match user fields defined in queue")
	}

	// append user data to args, if provided
	if len(q.userFieldNames) > 0 {
		args = append(args, m.customFieldData...)
	}

	_, err := conn.ExecContext(ctx, q.insertSQL, args...)
	return err
}

// getMessageForAdd gets a new message from the pool and sets defaults
func (q *Queue) getMessageForAdd() *Message {
	m := q.addPool.Get().(*Message)

	// set message defaults
	m.TimeScheduled = time.Now().UTC().UnixNano()
	m.Epoch = 100

	return m
}

// putMessageForAdd puts the message back into the pool
func (q *Queue) putMessageForAdd(m *Message) {
	q.addPool.Put(m)
}
