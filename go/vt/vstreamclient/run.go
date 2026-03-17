package vstreamclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"time"

	"google.golang.org/protobuf/proto"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// EventFunc is an optional callback function that can be registered for individual event types
type EventFunc func(ctx context.Context, event *binlogdatapb.VEvent) error

// FlushFunc is called per batch of rows, which could be any number of rows, but will be limited by the
// MaxRowsPerFlush setting. The rows are always a slice of the data type for the table, which is configured
// per table. You can use type assertion to convert the rows to the correct type, and then process them as needed.
//
//	func(ctx context.Context, rows any, meta FlushMeta) error {
//		 typedRows := rows.([]*DataType)
//		 // do something with the rows
//		 return nil
//	}
//
// Returning an error will stop the stream, and the last vgtid will not be stored. The stream will need to be
// restarted from the last successful flushed vgtid.
type FlushFunc func(ctx context.Context, rows []Row, meta FlushMeta) error

// Row is the data structure that will be passed to the FlushFunc. It contains the row event, the row change,
// and the scanned data itself. The data will be the type registered for the table; for delete events, it will be
// populated from the "before" image of the row.
type Row struct {
	RowEvent  *binlogdatapb.RowEvent
	RowChange *binlogdatapb.RowChange
	Data      any // populated as the data type registered for the table; for delete events, built from the "before" image
}

// FlushMeta is the metadata that is passed to the FlushFunc. It's not necessary, but might be useful
// for logging, debugging, etc.
type FlushMeta struct {
	Keyspace string
	Table    string

	TableStats   TableStats
	VStreamStats VStreamStats

	LatestVGtid *binlogdatapb.VGtid
	FlushReason FlushReason
}

type FlushReason int8

const (
	FlushReasonNone             FlushReason = 1
	FlushReasonCopyCompleted    FlushReason = 2
	FlushReasonMinDuration      FlushReason = 3
	FlushReasonMaxRowsPerFlush  FlushReason = 4
	FlushReasonGracefulShutdown FlushReason = 5
)

func (r FlushReason) String() string {
	switch r {
	case FlushReasonNone:
		return "none"
	case FlushReasonCopyCompleted:
		return "forced"
	case FlushReasonMinDuration:
		return "minDuration"
	case FlushReasonMaxRowsPerFlush:
		return "maxRows"
	case FlushReasonGracefulShutdown:
		return "gracefulShutdown"
	default:
		panic(fmt.Sprintf("unknown FlushReason: %d", r))
	}
}

// Run starts the VStream and processes events until the stream ends or an error occurs.
// A VStreamClient is single-use: after any Run call returns, create a new client for the next attempt.
//
// Run returns an error in these cases:
//   - Run is called on a client that was already used by a previous Run attempt
//   - the underlying VStream recv loop fails
//   - the context is canceled or times out while Run is still active
//   - a registered event hook returns an error
//   - row decoding or table lookup fails
//   - FlushFn returns an error
//   - checkpoint state updates fail
func (v *VStreamClient) Run(ctx context.Context) error {
	// make a cancelable context for GracefulShutdown to use, so it can signal the Run loop to exit
	ctx, cancelRunCtxFn := context.WithCancel(ctx)
	if !v.beginRun(cancelRunCtxFn) {
		cancelRunCtxFn()
		return errors.New("vstreamclient: client is closed; create a new client for each Run attempt")
	}
	defer cancelRunCtxFn()
	defer v.endRun()

	// initialize the streamer
	var err error
	v.reader, err = v.conn.VStream(ctx, v.tabletType, v.latestVgtid, v.filter, v.flags)
	if err != nil {
		return fmt.Errorf("vstreamclient: failed to create vstream: %w", err)
	}

	go v.listenForGracefulShutdown(ctx)

	go v.monitorHeartbeat(ctx)

	// to prevent an immediate flush, we initialize LastFlushedAt here, even if it wasn't technically flushed
	v.stats.LastFlushedAt = time.Now()

	// ********************************************************************************************************
	// Event Processing
	//
	// this is the main loop that processes events from the stream. It will continue until the stream ends or an
	// error occurs. The context is checked before processing each event.
	// ********************************************************************************************************
	for {
		// events come in batches, depending on how busy the keyspace is. This is where the network communication
		// happens, so it's the most likely place for errors to occur.
		var events []*binlogdatapb.VEvent
		events, err = v.reader.Recv()
		switch {
		case err == nil: // no error, continue processing below

		case errors.Is(err, io.EOF):
			return fmt.Errorf("vstreamclient: remote error: %w", io.ErrUnexpectedEOF)

		default:
			return fmt.Errorf("vstreamclient: remote error: %w", err)
		}

		err = v.handleEvents(ctx, events)
		if err != nil {
			return err
		}

		// this is also checked inside of handleEvents, but in case the flush function takes a long time, we'll check
		// here as well to make sure we can exit in a timely manner during shutdown or if the context is canceled
		// while processing events.
		var shouldExit bool
		shouldExit, err = v.shouldExitRun(ctx)
		if err != nil {
			return err
		}
		if shouldExit {
			return nil
		}
	}
}

func (v *VStreamClient) shouldExitRun(ctx context.Context) (bool, error) {
	// the select cases are executed separately, because if both the graceful shutdown flush channel is closed and
	// the context is canceled, we want to check the graceful shutdown first. That means we can exit without
	// returning an error, whereas if the context is canceled, we want to return the context error. They'll often both
	// happen around the same time, in which case the select would choose pseudo-randomly between the two.

	gracefulShutdownFlushChan := v.getGracefulShutdownFlushChan()

	select {
	// if we enter this case, that means the last flush completed during the shutdown process, and closed
	// the channel, so we can exit immediately without processing any more events. This is the happy
	// path for graceful shutdown, since we were able to flush all buffered data
	case <-gracefulShutdownFlushChan:
		return true, nil

	default:
	}

	select {
	// since any processing will likely be wasted, return early if the context is already done
	case <-ctx.Done():
		return true, ctx.Err()

	default:
	}

	return false, nil
}

func (v *VStreamClient) handleEvents(ctx context.Context, events []*binlogdatapb.VEvent) error {
	// if event processing / flushing takes longer than the heartbeat timeout, the heartbeat monitor might trigger
	// and cancel the context, since no events are processed during that time. This technically leaves a tiny race
	// condition between defer running and the next Recv happening in the Run() loop, but trying to make that
	// perfect would add a lot of complexity, and the window is very small.
	v.isProcessingEvents.Store(true)
	defer func() {
		// keep track of the last event time for heartbeat monitoring. We're purposefully not using the event
		// timestamp, since that would cause cancellation if the stream was copying, delayed, or lagging.
		v.lastEventProcessedAtUnixNano.Store(time.Now().UnixNano())
		v.isProcessingEvents.Store(false)
	}()

	for _, ev := range events {
		shouldExit, err := v.shouldExitRun(ctx)
		if err != nil {
			return err
		}
		if shouldExit {
			return nil
		}

		// call the user-defined event function if it exists
		fn, ok := v.eventFuncs[ev.Type]
		if ok {
			err = fn(ctx, ev)
			if err != nil {
				return fmt.Errorf("vstreamclient: user error processing %s event: %w", ev.Type.String(), err)
			}
		}

		// handle individual events based on their type
		switch ev.Type {
		// field events are sent first and contain schema information for any tables that are being streamed,
		// so we cache the fields for each table as they come in
		case binlogdatapb.VEventType_FIELD:
			var table *TableConfig
			table, err = v.lookupTable(ev.FieldEvent.TableName)
			if err != nil {
				return err
			}

			err = table.handleFieldEvent(ev.FieldEvent)
			if err != nil {
				return err
			}

		// row events are the actual data changes, and we'll process them based on the table name
		case binlogdatapb.VEventType_ROW:
			var table *TableConfig
			table, err = v.lookupTable(ev.RowEvent.TableName)
			if err != nil {
				return err
			}

			err = table.handleRowEvent(ev.RowEvent, &v.stats)
			if err != nil {
				return err
			}

		// vgtid events are sent periodically, and we'll store the latest vgtid for checkpointing. We may get
		// this mid-transaction, so we don't flush here, so we don't propagate a partial transaction that may
		// be rolled back.
		case binlogdatapb.VEventType_VGTID:
			v.latestVgtid = ev.Vgtid

		// commit and other events are safe to flush on, since they indicate the end of a transaction.
		// Otherwise, there's not much to do with these events.
		case binlogdatapb.VEventType_COMMIT, binlogdatapb.VEventType_OTHER:
			// only flush when we have an event that guarantees we're not flushing mid-transaction
			err = v.flush(ctx, false)
			if err != nil {
				return err
			}

		// DDL events are schema changes, and we might want to handle them differently than data events.
		// They are safe to flush on, since they indicate the end of a transaction. If you want to
		// transparently adjust the destination schema based on DDL events, you would do that here.
		case binlogdatapb.VEventType_DDL:
			err = v.flush(ctx, false)
			if err != nil {
				return err
			}

		// COPY_COMPLETED events come through once per keyspace/shard, and an additional fully completed event
		// as noted in go/vt/vtgate/endtoend/vstream_test.go:951.
		// "The arrival order of COPY_COMPLETED events with keyspace/shard is not constant.
		// On the other hand, the last event should always be a fully COPY_COMPLETED event."
		case binlogdatapb.VEventType_COPY_COMPLETED:
			if isFinalCopyCompletedEvent(ev) {
				// The final copy boundary must checkpoint any buffered copy rows before
				// we persist copy_completed, or a crash can drop them permanently.
				err = v.flush(ctx, true)
				if err != nil {
					return err
				}
			}

		// heartbeat events are sent periodically if the source keyspace is idle and there are no other events.
		// It's possible that there is still buffered data that hadn't exceeded the min duration or max rows
		// thresholds the last time flush was called. Most of the time, that won't be the case, but flush will
		// check for that and only run if necessary.
		case binlogdatapb.VEventType_HEARTBEAT:
			err = v.flush(ctx, false)
			if err != nil {
				return err
			}

		// even if there are no changes to the tables being streamed, we'll still get a begin, vgtid, and commit
		// event for each transaction. The other two are used for checkpoints, but nothing to do here.
		case binlogdatapb.VEventType_BEGIN:

		// journal events are sent on resharding. Unless you are manually targeting shards, vstream should
		// transparently handle resharding for you, so you shouldn't need to do anything with these events.
		// You might want to log them for debugging purposes or to alert on resharding events in case
		// something goes wrong. After resharding, if the pre-reshard vgtid is no longer valid, you may need
		// to restart the stream from the beginning.
		case binlogdatapb.VEventType_JOURNAL:

		// there aren't strong cases for handling these events, but you might want to log them for debugging
		case binlogdatapb.VEventType_VERSION, binlogdatapb.VEventType_LASTPK, binlogdatapb.VEventType_SAVEPOINT:
		}
	}

	return nil
}

func isFinalCopyCompletedEvent(ev *binlogdatapb.VEvent) bool {
	return ev.Type == binlogdatapb.VEventType_COPY_COMPLETED && ev.Keyspace == "" && ev.Shard == ""
}

// ********************************************************************************************************
// Graceful Shutdown
//
// listenForGracefulShutdown waits for either a configured shutdown channel or configured OS signals,
// then initiates a graceful shutdown of the VStreamClient. It returns early if the provided context
// is canceled or times out.
// ********************************************************************************************************
func (v *VStreamClient) listenForGracefulShutdown(ctx context.Context) {
	if v.gracefulShutdownChan == nil && len(v.gracefulShutdownSignals) == 0 {
		return
	}

	var signalChan chan os.Signal
	if len(v.gracefulShutdownSignals) > 0 {
		signalChan = make(chan os.Signal, 1)
		signal.Notify(signalChan, v.gracefulShutdownSignals...)
		defer signal.Stop(signalChan)
	}

	select {
	case <-v.gracefulShutdownChan:
		v.GracefulShutdown(v.gracefulShutdownWaitDur)

	case <-signalChan:
		v.GracefulShutdown(v.gracefulShutdownWaitDur)

	case <-ctx.Done():
	}
}

// ********************************************************************************************************
// Heartbeat Monitoring
//
// the heartbeat ticker is used to ensure that we haven't been disconnected from the stream after the stream has
// started delivering events. Before the first event arrives, heartbeat liveness checks stay disabled so a slow but
// healthy startup does not self-cancel.
// ********************************************************************************************************
func (v *VStreamClient) monitorHeartbeat(ctx context.Context) {
	const startupTimeout = 5 * time.Minute
	const timeoutMultiplier = 2

	heartbeatDur := time.Duration(v.flags.HeartbeatInterval) * time.Second
	heartbeat := time.NewTicker(heartbeatDur)
	defer heartbeat.Stop()

	startupTimer := time.NewTimer(startupTimeout)
	startupTimerChan := startupTimer.C

	for {
		select {
		case tm := <-heartbeat.C:
			// if we haven't processed any events yet, we should skip the heartbeat check, since it's likely that
			// we're still starting up and haven't had a chance to receive the first event yet. This is especially
			// important if the startup is slow, since we don't want to accidentally shut down during startup.
			lastEventProcessedAtUnixNano := v.lastEventProcessedAtUnixNano.Load()
			if lastEventProcessedAtUnixNano == 0 {
				continue
			}

			// once we receive the first event, we can stop the startup timer and disable the select case
			if startupTimerChan != nil {
				startupTimer.Stop()
				startupTimerChan = nil
			}

			// if we're currently processing events, we should skip the heartbeat check, since it's likely that we're
			// just busy and haven't had a chance to receive the latest event yet. This is especially important for
			// long-running flushes since they can take longer than the heartbeat duration, and we don't want to
			// accidentally cancel the context during a flush.
			if v.isProcessingEvents.Load() {
				continue
			}

			// if we haven't received an event in twice the heartbeat duration, we'll cancel the context, since
			// we're likely disconnected, and exit the goroutine
			if tm.Sub(time.Unix(0, lastEventProcessedAtUnixNano)) > heartbeatDur*timeoutMultiplier {
				v.GracefulShutdown(v.gracefulShutdownWaitDur)
				return
			}

		case <-startupTimerChan:
			// this is a sanity check to shutdown the client if we never receive a single event
			if v.lastEventProcessedAtUnixNano.Load() == 0 {
				panic("vstreamclient: vstream never received an event")
			}
			startupTimerChan = nil

		case <-ctx.Done():
			return
		}
	}
}

// ********************************************************************************************************
// Flush Data + Store Vgtid
//
// as we process events, we'll periodically need to checkpoint the last vgtid and persist the buffered data to
// storage. You can control the frequency of this flush by adjusting the minFlushDuration and maxRowsPerFlush.
// This is only called when we have an event that guarantees we're not flushing mid-transaction.
//
// The isCopyCompleted flag is used to indicate that we've received the final COPY_COMPLETED event, which means
// we should flush all remaining data, even if it doesn't meet the usual thresholds for flushing. This ensures
// that all copied data is flushed and checkpointed before we mark the stream as fully copied. It also sets
// copy_completed to true in the state table.
// ********************************************************************************************************
func (v *VStreamClient) flush(ctx context.Context, isCopyCompleted bool) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("vstreamclient: context error before flush: %w", err)
	}

	hasBufferedRows := v.hasBufferedRows()

	// if the lastFlushedVgtid is the same as the latestVgtid and there is no buffered data,
	// we don't need to do anything.
	if !isCopyCompleted && !hasBufferedRows && proto.Equal(v.lastFlushedVgtid, v.latestVgtid) {
		v.signalGracefulShutdownFlushed()
		return nil
	}

	shouldFlush, flushReason := v.shouldFlush(hasBufferedRows, isCopyCompleted)
	if !shouldFlush {
		return nil
	}

	// TODO: maybe start a transaction here, and pass it to the flush function

	for _, table := range v.tables {
		// flush the rows to the database, chunked using the max batch size
		for chunk := range slices.Chunk(table.currentBatch, table.MaxRowsPerFlush) {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("vstreamclient: context error during flush: %w", err)
			}

			err := table.FlushFn(ctx, chunk, FlushMeta{
				Keyspace: table.Keyspace,
				Table:    table.Table,

				TableStats:   table.stats,
				VStreamStats: v.stats,

				LatestVGtid: v.latestVgtid,
				FlushReason: flushReason,
			})
			if err != nil {
				return fmt.Errorf("vstreamclient: error flushing table %s: %w", table.Table, err)
			}

			// update the stats for the table and the vstream
			table.stats.FlushCount++
			table.stats.FlushedRowCount += len(chunk)
			table.stats.LastFlushedAt = time.Now()

			v.stats.TableFlushCount++
			v.stats.FlushedRowCount += len(chunk)
		}

		table.resetBatch()
	}

	// always store the latest vgtid, even if there are no rows to store
	err := updateLatestVGtid(ctx, v.session, v.name, v.vgtidStateKeyspace, v.vgtidStateTable, v.latestVgtid, isCopyCompleted)
	if err != nil {
		return err
	}

	v.stats.FlushCount++
	v.stats.LastFlushedAt = time.Now()
	v.lastFlushedVgtid = v.latestVgtid

	// if we're in the middle of a graceful shutdown, and we've successfully flushed all the remaining data,
	// we can close the channel to let the Run loop know it can exit
	v.signalGracefulShutdownFlushed()

	return nil
}

func (v *VStreamClient) hasBufferedRows() bool {
	for _, table := range v.tables {
		if len(table.currentBatch) > 0 {
			return true
		}
	}

	return false
}

func (v *VStreamClient) shouldFlush(hasBufferedRows, isCopyCompleted bool) (bool, FlushReason) {
	if isCopyCompleted {
		return true, FlushReasonCopyCompleted
	}

	// if we have exceeded the minFlushDuration, we'll force a flush, regardless how many rows each table has
	if time.Since(v.stats.LastFlushedAt) > v.minFlushDuration {
		return true, FlushReasonMinDuration
	}

	// if we haven't exceeded the min flush duration, we'll check if any of the tables have exceeded their
	// max rows to flush. If any of them have, we will force a flush. Every table needs to be flushed at
	// the same time, since the last vgtid covers all tables.
	if hasBufferedRows {
		for _, table := range v.tables {
			if len(table.currentBatch) >= table.MaxRowsPerFlush {
				return true, FlushReasonMaxRowsPerFlush
			}
		}
	}

	// even if we haven't hit either of minDuration or maxRows, if we're actively closing
	// down, go ahead and force the flush.
	if v.isShutdownRequested() {
		return true, FlushReasonGracefulShutdown
	}

	return false, FlushReasonNone
}
