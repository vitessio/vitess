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

type receiverWithStatus struct {
	receiver MessageReceiver
	busy     bool
}

// MessageManager manages messages for a message table.
type MessageManager struct {
	DBLock sync.Mutex
	tsv    *TabletServer

	isOpen bool

	name        string
	ackWaitTime time.Duration
	purgeAfter  time.Duration
	batchSize   int
	pollerTicks *timer.Timer
	purgeTicks  *timer.Timer
	connpool    *ConnPool

	mu sync.Mutex
	// cond gets triggered if a receiver becomes available (curReceiver != -1),
	// an item gets added to the cache, or if the manager is closed.
	// The trigger wakes up the runSend thread.
	cond            sync.Cond
	cache           *MessagerCache
	receivers       []*receiverWithStatus
	curReceiver     int
	messagesPending bool

	// wg is for ensuring all running goroutines have returned
	// before we can close the manager.
	wg sync.WaitGroup

	readByTimeNext *sqlparser.ParsedQuery
	ackQuery       *sqlparser.ParsedQuery
	postponeQuery  *sqlparser.ParsedQuery
	purgeQuery     *sqlparser.ParsedQuery
}

// NewMessageManager creates a new message manager.
func NewMessageManager(tsv *TabletServer, name string, ackWaitTime, purgeAfter time.Duration, batchSize, cacheSize int, pollInterval time.Duration, connpool *ConnPool) *MessageManager {
	mm := &MessageManager{
		tsv:         tsv,
		name:        name,
		ackWaitTime: ackWaitTime,
		purgeAfter:  purgeAfter,
		batchSize:   batchSize,
		cache:       NewMessagerCache(cacheSize),
		pollerTicks: timer.NewTimer(pollInterval),
		purgeTicks:  timer.NewTimer(pollInterval),
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
	mm.postponeQuery = buildParsedQuery(
		"update %v set time_next = %a+(%a<<epoch), epoch = epoch+1 where id in %a and time_acked is null",
		tableName, ":time_now", ":wait_time", "::ids")
	mm.purgeQuery = buildParsedQuery(
		"delete from %v where time_scheduled < %a and time_acked is not null limit 500",
		tableName, ":time_scheduled")
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
	mm.pollerTicks.Start(mm.runPoller)
	mm.purgeTicks.Start(mm.runPurge)
}

// Close stops the MessageManager service.
func (mm *MessageManager) Close() {
	mm.pollerTicks.Stop()
	mm.purgeTicks.Stop()

	mm.mu.Lock()
	if !mm.isOpen {
		mm.mu.Unlock()
		return
	}
	mm.isOpen = false
	for _, rcvr := range mm.receivers {
		rcvr.receiver.Cancel()
	}
	mm.receivers = nil
	mm.cache.Clear()
	mm.cond.Broadcast()
	mm.mu.Unlock()

	mm.wg.Wait()
}

// Subscribe adds the receiver to the list of subsribers.
func (mm *MessageManager) Subscribe(receiver MessageReceiver) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	for _, rcv := range mm.receivers {
		if rcv.receiver == receiver {
			return
		}
	}
	mm.receivers = append(mm.receivers, &receiverWithStatus{receiver: receiver})
	if mm.curReceiver == -1 {
		mm.curReceiver = len(mm.receivers) - 1
		mm.cond.Broadcast()
	}
}

// Unsubscribe removes the receiver from the list of subscribers.
func (mm *MessageManager) Unsubscribe(receiver MessageReceiver) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	for i, rcv := range mm.receivers {
		if rcv.receiver != receiver {
			continue
		}
		// Delete the item at current position.
		n := len(mm.receivers)
		copy(mm.receivers[i:n-1], mm.receivers[i+1:n])
		mm.receivers = mm.receivers[0 : n-1]
		break
	}
	// curReceiver is obsolete. Recompute.
	mm.rescanReceivers(-1)
	// If there are no receivers. Shut down the cache.
	if len(mm.receivers) == 0 {
		mm.cache.Clear()
	}
}

// rescanReceivers finds the next available receiver
// using start as the starting point. If one was found,
// it sets curReceiver to that index. If curReceiver
// was previously -1, it broadcasts. If none was found,
// curReceiver is set to -1. If there's no starting point,
// it must be specified as -1.
func (mm *MessageManager) rescanReceivers(start int) {
	cur := start
	for range mm.receivers {
		cur = (cur + 1) % len(mm.receivers)
		if !mm.receivers[cur].busy {
			if mm.curReceiver == -1 {
				mm.cond.Broadcast()
			}
			mm.curReceiver = cur
			return
		}
	}
	// Nothing was found.
	mm.curReceiver = -1
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
	mm.cond.Broadcast()
	return true
}

func (mm *MessageManager) runSend() {
	defer mm.wg.Done()
	for {
		var mrs []*MessageRow
		mm.mu.Lock()
		for {
			if !mm.isOpen {
				mm.mu.Unlock()
				return
			}
			if mm.curReceiver == -1 {
				mm.cond.Wait()
				continue
			}
			for i := 0; i < mm.batchSize; i++ {
				if mr := mm.cache.Pop(); mr != nil {
					mrs = append(mrs, mr)
					continue
				}
				break
			}
			if mrs != nil {
				break
			}
			if mm.messagesPending {
				// Trigger the poller to fetch more.
				mm.pollerTicks.Trigger()
			}
			mm.cond.Wait()
		}
		// If we're here, there is a current receiver, and a message
		// to send. Reserve the receiver and find the next one.
		receiver := mm.receivers[mm.curReceiver]
		receiver.busy = true
		mm.rescanReceivers(mm.curReceiver)
		mm.mu.Unlock()
		// Send the message asynchronously.
		go mm.send(receiver, mrs)
	}
}

func (mm *MessageManager) send(receiver *receiverWithStatus, mrs []*MessageRow) {
	if err := receiver.receiver.Send(mm.name, mrs); err != nil {
		log.Errorf("Error sending messages: %v", mrs)
	}
	mm.mu.Lock()
	receiver.busy = false
	if mm.curReceiver == -1 {
		// Since the current receiver became non-busy,
		// rescan.
		mm.rescanReceivers(-1)
	}
	mm.mu.Unlock()
	ids := make([]string, len(mrs))
	for i, mr := range mrs {
		ids[i] = mr.id
	}
	// Postpone the messages for resend before discarding
	// from cache. If no timely ack is received, it will be resent.
	mm.postpone(ids)
	// postpone should discard, but this is a safety measure
	// in case it fails.
	mm.cache.Discard(ids)
}

func (mm *MessageManager) postpone(ids []string) {
	ctx, cancel := context.WithTimeout(localContext(), mm.ackWaitTime)
	defer cancel()
	_, err := mm.tsv.PostponeMessages(ctx, nil, mm.name, ids)
	if err != nil {
		// TODO(sougou): increment internal error.
		log.Errorf("Unable to postpone messages %v: %v", ids, err)
	}
}

func (mm *MessageManager) runPoller() {
	ctx, cancel := context.WithTimeout(localContext(), mm.pollerTicks.Interval())
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
			defer mm.cond.Broadcast()
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
}

func (mm *MessageManager) runPurge() {
	ctx, cancel := context.WithTimeout(localContext(), mm.purgeTicks.Interval())
	defer cancel()
	for {
		count, err := mm.tsv.PurgeMessages(ctx, nil, mm.name, time.Now().Add(-mm.purgeAfter).UnixNano())
		if err != nil {
			// TODO(sougou): increment internal error.
			log.Errorf("Unable to delete messages: %v", err)
		}
		// If deleted 500 or more, we should continue.
		if count < 500 {
			return
		}
	}
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

// GeneratePostponeQuery returns the query and bind vars for postponing a message.
func (mm *MessageManager) GeneratePostponeQuery(ids []string) (string, map[string]interface{}) {
	idbvs := make([]interface{}, len(ids))
	for i, id := range ids {
		idbvs[i] = id
	}
	return mm.postponeQuery.Query, map[string]interface{}{
		"time_now":  int64(time.Now().UnixNano()),
		"wait_time": int64(mm.ackWaitTime),
		"ids":       idbvs,
	}
}

// GeneratePurgeQuery returns the query and bind vars for purging messages.
func (mm *MessageManager) GeneratePurgeQuery(timeCutoff int64) (string, map[string]interface{}) {
	return mm.purgeQuery.Query, map[string]interface{}{
		"time_scheduled": timeCutoff,
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
