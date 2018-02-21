package messages

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/vitessdriver"
)

// Subscription allows users to interact with the queue
type Subscription struct {
	q  *Queue
	mu sync.Mutex

	// this is true while a Subscription is live
	isActive bool

	cancelFunc context.CancelFunc

	// Subscribe requires unique connection string properties,
	// so we will manage that on behalf of the user
	db *sql.DB

	// store db connection details
	dbConfig vitessdriver.Configuration

	// this stores all of the in flight messages
	msgBuf []Message

	// these buffered channels manage max message processing concurrency

	// these messages are ready to be filled with data from the db
	waitingForDataChan chan *Message

	// these messages are waiting for a subscriber to process them
	readyForProcessingChan chan *Message

	streamSQL string
	ackSQL    string
	failSQL   string
}

// Get returns the next available message. It blocks until either a message
// is available or the context is cancelled
func (s *Subscription) Get(ctx context.Context) (*Message, error) {
	select {
	case m := <-s.readyForProcessingChan:
		if m.err != nil {
			return nil, m.err
		}
		return m, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Ack marks a message as successfully completed
func (m *Message) Ack(ctx context.Context) error {
	defer m.s.putMessage(m)
	_, err := m.s.db.ExecContext(ctx, m.s.ackSQL, time.Now().UTC().UnixNano(), m.timeScheduled, m.ID)
	return err
}

// Fail marks a task as failed, and it will not be queued again until manual action is taken
func (m *Message) Fail(ctx context.Context) error {
	defer m.s.putMessage(m)
	_, err := m.s.db.ExecContext(ctx, m.s.failSQL, m.timeScheduled, m.ID)
	return err
}

// Subscribe returns a subscription
// Context cancellation is respected
func (q *Queue) Subscribe(ctx context.Context, address, target string) (*Subscription, error) {
	q.s.mu.Lock()
	defer q.s.mu.Unlock()

	if q.s.isActive {
		return q.s, nil
	}

	// generate the raw subscription
	q.s = q.newSubscription(q.maxConcurrent)

	// create sub-context for subscribe goroutine with cancelFunc for cleanup
	var newCtx context.Context
	newCtx, q.s.cancelFunc = context.WithCancel(ctx)

	// open a direct database connection if needed
	if err := q.s.openDB(newCtx, address, target); err != nil {
		return nil, err
	}

	// start a streaming query that will run indefinitely
	rows, err := q.s.db.QueryContext(newCtx, q.s.streamSQL)
	if err != nil {
		return nil, err
	}

	// this goroutine will be waiting for rows and is the only writer to the channel
	go func() {
		defer rows.Close()

		// we don't need to check for context cancellation, because that is already
		// happening behind the scenes in the Vitess database/sql driver
		for rows.Next() {
			m := <-q.s.waitingForDataChan
			m.err = rows.Scan(m.scanFields...)

			// send the message through the channel
			q.s.readyForProcessingChan <- m
		}

		// close the channel before exiting
		close(q.s.readyForProcessingChan)
	}()

	return q.s, nil
}

func (q *Queue) newSubscription(maxConcurrent int) *Subscription {
	s := &Subscription{
		q:                      q,
		isActive:               false,
		mu:                     sync.Mutex{},
		msgBuf:                 make([]Message, maxConcurrent),
		waitingForDataChan:     make(chan *Message, maxConcurrent),
		readyForProcessingChan: make(chan *Message, maxConcurrent),
		streamSQL:              fmt.Sprintf("stream * from `%s`", q.name),
		ackSQL:                 fmt.Sprintf("UPDATE `%s` SET time_acked=?, time_next=null WHERE time_scheduled=? AND id=? AND time_acked is null", q.name),
		failSQL:                fmt.Sprintf("UPDATE `%s` SET time_next=%d WHERE time_scheduled=? AND id=? AND time_acked is null", q.name, math.MaxInt64),
	}

	// initialize all the individual messages
	// each message will be reset as it is reused
	for i := range s.msgBuf {
		// create a pointer to the message for convenience
		m := &s.msgBuf[i]

		// add a reference to the original queue
		m.s = s

		// create a permanent set of scan fields
		m.scanFields = []interface{}{
			&m.timeScheduled,
			&m.ID,
		}

		// if the user has set up custom fields, initialize them
		if q.userFieldNames != nil {
			m.customFieldData = q.newFieldsFunc()

			// add a pointer to each of the individual user fields to scanFields
			for j := range m.customFieldData {
				m.scanFields = append(m.scanFields, &m.customFieldData[j])
			}
		}
	}

	return s
}

// putMessage pushes a message back into the queue for resue
func (s *Subscription) putMessage(m *Message) {
	s.waitingForDataChan <- m
}

func (s *Subscription) openDB(ctx context.Context, address, target string) error {
	s.dbConfig = vitessdriver.Configuration{
		Address:   address,
		Target:    target,
		Streaming: true,
	}

	var err error
	s.db, err = vitessdriver.OpenWithConfiguration(s.dbConfig)
	if err != nil {
		return err
	}
	s.db.SetMaxOpenConns(1)
	s.db.SetMaxIdleConns(1)

	return nil
}

// Close drains the processing channel and closes the connection to the database
// TODO: Nack all remaining messages
func (s *Subscription) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.isActive = false

	// cancel context inside rows.Next()
	s.cancelFunc()

	// drain processing channel
	for range s.readyForProcessingChan {
	}

	// drain waiting channel
	for range s.waitingForDataChan {
	}

	return s.db.Close()
}
