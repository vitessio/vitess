// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

// MessageManager manages messages for a message table.
type MessageManager struct {
	DBLock sync.Mutex
	tsv    *TabletServer

	isOpen bool

	name        string
	ackWaitTime time.Duration
	cache       *MessagerCache
	ticks       *timer.Timer
	connpool    *ConnPool

	mu sync.Mutex
	// cond gets triggered if len(receivers) becomes non-zero,
	// or an item gets added to the cache, or if the manager is closed.
	// The trigger wakes up the runSend thread.
	cond            sync.Cond
	receivers       []MessageReceiver
	curReceiver     int
	messagesPending bool

	// wg is for ensuring all running goroutines have returned
	// before we can close the manager.
	wg sync.WaitGroup

	readByTimeNext  *sqlparser.ParsedQuery
	ackQuery        *sqlparser.ParsedQuery
	rescheduleQuery *sqlparser.ParsedQuery
}

// NewMessageManager creates a new message manager.
func NewMessageManager(tsv *TabletServer, name string, ackWaitTime time.Duration, cacheSize int, pollInterval time.Duration, connpool *ConnPool) *MessageManager {
	mm := &MessageManager{
		tsv:         tsv,
		name:        name,
		ackWaitTime: ackWaitTime,
		cache:       NewMessagerCache(cacheSize),
		ticks:       timer.NewTimer(pollInterval),
		connpool:    connpool,
	}
	mm.cond.L = &mm.mu

	tableName := sqlparser.NewTableIdent(name)
	mm.readByTimeNext = buildParsedQuery(
		"select time_next, epoch, id, message from %v where time_next < %a order by time_next desc limit %a",
		tableName, ":time_next", ":max")
	mm.ackQuery = buildParsedQuery(
		"update %v set time_acked = %a, time_next = null where id in %a",
		tableName, ":time_acked", "::ids")
	mm.rescheduleQuery = buildParsedQuery(
		"update %v set time_next = %a, epoch = epoch+1 where id in %a and time_acked is null",
		tableName, ":time_next", "::ids")
	return mm
}

// Open starts the MessageManager service.
func (mm *MessageManager) Open() {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	if mm.isOpen {
		return
	}
	mm.isOpen = true
	mm.wg.Add(1)
	go mm.runSend()
	// TODO(sougou): improve ticks to add randomness.
	mm.ticks.Start(mm.runPoller)
}

// Close stops the MessageManager service.
func (mm *MessageManager) Close() {
	mm.ticks.Stop()

	mm.mu.Lock()
	if !mm.isOpen {
		mm.mu.Unlock()
		return
	}
	mm.isOpen = false
	for _, rcvr := range mm.receivers {
		rcvr.Cancel()
	}
	mm.receivers = nil
	mm.cache.Clear()
	mm.cond.Signal()
	mm.mu.Unlock()

	mm.wg.Wait()
}

// Subscribe adds the receiver to the list of subsribers.
func (mm *MessageManager) Subscribe(receiver MessageReceiver) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	for _, rcv := range mm.receivers {
		if rcv == receiver {
			return
		}
	}
	mm.receivers = append(mm.receivers, receiver)
	mm.cond.Signal()
}

// Unsubscribe removes the receiver from the list of subscribers.
func (mm *MessageManager) Unsubscribe(receiver MessageReceiver) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	for i, rcv := range mm.receivers {
		if rcv != receiver {
			continue
		}
		// Delete the item at current position.
		n := len(mm.receivers)
		copy(mm.receivers[i:n-1], mm.receivers[i+1:n])
		mm.receivers = mm.receivers[0 : n-1]
		break
	}
	if len(mm.receivers) == 0 {
		mm.cache.Clear()
	}
}

// Add adds the message to the cache. It returns true
// if successful. If the message is already present,
// it still returns true.
func (mm *MessageManager) Add(mr *MessageRow) bool {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	if len(mm.receivers) == 0 {
		return false
	}
	if !mm.cache.Add(mr) {
		return false
	}
	mm.cond.Signal()
	return true
}

func (mm *MessageManager) runSend() {
	defer mm.wg.Done()
	var mr *MessageRow
	for {
		mm.mu.Lock()
		for {
			if !mm.isOpen {
				mm.mu.Unlock()
				return
			}
			if len(mm.receivers) == 0 {
				mm.cond.Wait()
				continue
			}
			if mr = mm.cache.Pop(); mr != nil {
				break
			}
			if mm.messagesPending {
				// Trigger the poller to fetch more.
				mm.ticks.Trigger()
			}
			mm.cond.Wait()
		}
		mm.curReceiver = (mm.curReceiver + 1) % len(mm.receivers)
		receiver := mm.receivers[mm.curReceiver]
		mm.mu.Unlock()

		_ = receiver.Send(mm.name, mr)
		mm.postpone(mr)
		mm.cache.Discard(mr.id)
	}
}

func (mm *MessageManager) postpone(mr *MessageRow) {
	ctx, cancel := context.WithTimeout(context.Background(), mm.ackWaitTime)
	defer cancel()
	newTime := time.Now().Add(mm.ackWaitTime << uint64(mr.Epoch)).UnixNano()
	_, err := mm.tsv.RescheduleMessages(ctx, &mm.tsv.target, mm.name, []string{mr.id}, newTime)
	if err != nil {
		// TODO(sougou): increment internal error.
		log.Errorf("Unable to postpone message %v: %v", mr, err)
	}
}

func (mm *MessageManager) runPoller() {
	ctx, cancel := context.WithTimeout(context.Background(), mm.ticks.Interval())
	defer cancel()
	conn, err := mm.connpool.Get(ctx)
	if err != nil {
		// TODO(sougou): increment internal error.
		log.Errorf("Error getting connection: %v", err)
		return
	}
	defer conn.Recycle()

	func() {
		// Fast-path. Skip all the work.
		if mm.receiverCount() == 0 {
			return
		}
		mm.DBLock.Lock()
		defer mm.DBLock.Unlock()

		size := mm.cache.Size()
		bindVars := map[string]interface{}{
			"time_next": int64(time.Now().UnixNano()),
			"max":       int64(size),
		}
		qr, err := mm.read(ctx, conn, mm.readByTimeNext, bindVars)
		if err != nil {
			return
		}

		// Obtain mu lock to verify and preserve that len(receivers) != 0.
		mm.mu.Lock()
		defer mm.mu.Unlock()
		mm.messagesPending = false
		if len(qr.Rows) >= size {
			// There are probably more messages to be sent.
			mm.messagesPending = true
		}
		if len(mm.receivers) == 0 {
			// Almost never reachable because we just checked this.
			return
		}
		if len(qr.Rows) != 0 {
			// We've most likely added items.
			// Wake up the sender.
			defer mm.cond.Signal()
		}
		for _, row := range qr.Rows {
			mr, err := BuildMessageRow(row)
			if err != nil {
				// TODO(sougou): increment internal error.
				log.Errorf("Error reading message row: %v", err)
				continue
			}
			if !mm.cache.Add(mr) {
				mm.messagesPending = true
				return
			}
		}
	}()

	// TODO(sougou): purge.
}

// GenerateAckQuery returns the query and bind vars for acking a message.
func (mm *MessageManager) GenerateAckQuery(ids []string) (string, map[string]interface{}) {
	idbvs := make([]interface{}, len(ids))
	for i, id := range ids {
		idbvs[i] = id
	}
	return mm.ackQuery.Query, map[string]interface{}{
		"time_acked": int64(time.Now().UnixNano()),
		"ids":        idbvs,
	}
}

// GenerateRescheduleQuery returns the query and bind vars for rescheduling a message.
func (mm *MessageManager) GenerateRescheduleQuery(ids []string, timeNew int64) (string, map[string]interface{}) {
	idbvs := make([]interface{}, len(ids))
	for i, id := range ids {
		idbvs[i] = id
	}
	return mm.rescheduleQuery.Query, map[string]interface{}{
		"time_next": timeNew,
		"ids":       idbvs,
	}
}

// BuildMessageRow builds a MessageRow for a db row.
func BuildMessageRow(row []sqltypes.Value) (*MessageRow, error) {
	timeNext, err := row[0].ParseInt64()
	if err != nil {
		return nil, err
	}
	epoch, err := row[1].ParseInt64()
	if err != nil {
		return nil, err
	}
	return &MessageRow{
		TimeNext: timeNext,
		Epoch:    epoch,
		ID:       row[2],
		Message:  row[3],
		id:       row[2].String(),
	}, nil
}

func (mm *MessageManager) receiverCount() int {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	return len(mm.receivers)
}

func (mm *MessageManager) read(ctx context.Context, conn *DBConn, pq *sqlparser.ParsedQuery, bindVars map[string]interface{}) (*sqltypes.Result, error) {
	b, err := pq.GenerateQuery(bindVars)
	if err != nil {
		// TODO(sougou): increment internal error.
		log.Errorf("Error reading rows from message table: %v", err)
		return nil, err
	}
	return conn.Exec(ctx, string(b), mm.cache.Size()+1, false)
}
