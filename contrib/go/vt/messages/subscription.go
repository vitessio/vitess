package messages

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sync"

	"github.com/youtube/vitess/go/vt/vitessdriver"
)

// subscription allows users to interact with the queue
type subscription struct {
	mu sync.RWMutex

	// this is true while a Subscription is live
	isOpen bool

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

// Open connects to an underlying Vitess cluster and streams messages. The queue will
// buffer the defined max concurrent number of messages in memory and will block until
// one of the messages is acknowledged.
//  using the vitessdriver for database/sql.
// Only a single connection is opened and it remains open until Close is called.
// Context cancellation is respected
func (q *Queue) Open(ctx context.Context, address, target string) error {
	q.s.mu.Lock()
	defer q.s.mu.Unlock()

	if q.s.isOpen {
		return nil
	}

	// generate the raw subscription
	q.s = q.newSubscription(q.maxConcurrent)

	// create sub-context for subscribe goroutine with cancelFunc for cleanup
	var newCtx context.Context
	newCtx, q.s.cancelFunc = context.WithCancel(ctx)

	// open a direct database connection if needed
	if err := q.s.openDB(newCtx, address, target); err != nil {
		return err
	}

	// start a streaming query that will run indefinitely
	rows, err := q.s.db.QueryContext(newCtx, q.s.streamSQL)
	if err != nil {
		return err
	}

	// this goroutine will be waiting for rows and is the only writer to the channel
	go func() {
		defer rows.Close()

		// we don't need to check for context cancellation, because that is already
		// happening behind the scenes in the Vitess database/sql driver
		for rows.Next() {
			m := <-q.s.waitingForDataChan
			m.err = rows.Scan(m.dataPointers...)

			// send the message through the channel
			q.s.readyForProcessingChan <- m
		}

		// close the channel before exiting
		close(q.s.readyForProcessingChan)
	}()

	return nil
}

func (q *Queue) newSubscription(maxConcurrent int) *subscription {
	s := &subscription{
		isOpen:                 false,
		mu:                     sync.RWMutex{},
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
		m.dataPointers = []interface{}{
			&m.timeScheduled,
			&m.ID,
		}

		// if the user has set up custom fields, initialize them
		if q.fieldNames != nil {
			m.Data = q.destFunc()

			// add a pointer to each of the individual user fields to scanFields
			for j := range m.Data {
				m.dataPointers = append(m.dataPointers, &m.Data[j])
			}
		}
	}

	return s
}

func (s *subscription) openDB(ctx context.Context, address, target string) error {
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
func (q *Queue) Close() error {
	q.s.mu.Lock()
	defer q.s.mu.Unlock()

	// if the connection isn't open, no further work required
	if !q.s.isOpen {
		return nil
	}

	q.s.isOpen = false

	// cancel context inside rows.Next()
	q.s.cancelFunc()

	// drain processing channel
	for range q.s.readyForProcessingChan {
	}

	// drain waiting channel
	for range q.s.waitingForDataChan {
	}

	return q.s.db.Close()
}
