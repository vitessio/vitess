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
}

// Run starts the VStream and processes events until the stream ends or an error occurs.
// After GracefulShutdown is called, the client is closed and Run will return an error.
//
// Run returns an error in these cases:
//   - Run is called after GracefulShutdown already closed the client
//   - the underlying VStream recv loop fails
//   - the context is canceled or times out while Run is still active
//   - a registered event hook returns an error
//   - row decoding or table lookup fails
//   - FlushFn returns an error
//   - checkpoint state updates fail
func (v *VStreamClient) Run(ctx context.Context) error {
	if v.isClosing.Load() {
		return errors.New("vstreamclient: client is closed; create a new client after GracefulShutdown")
	}

	// make a cancelable context for GracefulShutdown to use, so it can signal the Run loop to exit
	ctx, v.cancelRunCtxFn = context.WithCancel(ctx)
	defer v.cancelRunCtxFn()

	// initialize the streamer
	var err error
	v.reader, err = v.conn.VStream(ctx, v.tabletType, v.latestVgtid, v.filter, v.flags)
	if err != nil {
		return fmt.Errorf("vstreamclient: failed to create vstream: %w", err)
	}

	go v.listenForGracefulShutdown(ctx)

	go v.monitorHeartbeat(ctx)

	// ********************************************************************************************************
	// Event Processing
	//
	// this is the main loop that processes events from the stream. It will continue until the stream ends or an
	// error occurs. The context is checked before processing each event.
	// ********************************************************************************************************
	for {
		// events come in batches, depending on how busy the keyspace is. This is where the network communication
		// happens, so it's the most likely place for errors to occur.
		events, err := v.reader.Recv()
		switch {
		case err == nil: // no error, continue processing below

		case errors.Is(err, io.EOF):
			return nil

		default:
			return fmt.Errorf("vstreamclient: remote error: %w", err)
		}

		for _, ev := range events {
			select {
			// if we enter this case, that means the last flush completed during the shutdown process, and closed
			// the channel, so we can exit immediately without processing any more events. This is the happy
			// path for graceful shutdown, since we were able to flush all buffered data
			case <-v.gracefulShutdownFlushChan:
				return nil

			// since any processing will likely be wasted, return early if the context is already done
			case <-ctx.Done():
				return ctx.Err()

			// if neither of those cases is true, we'll continue processing the event as normal
			default:
			}

			// keep track of the last event time for heartbeat monitoring. We're purposefully not using the event
			// timestamp, since that would cause cancellation if the stream was copying, delayed, or lagging.
			v.lastEventReceivedAtUnixNano.Store(time.Now().UnixNano())

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
				err = v.flush(ctx)
				if err != nil {
					return err
				}

			// DDL events are schema changes, and we might want to handle them differently than data events.
			// They are safe to flush on, since they indicate the end of a transaction. If you want to
			// transparently adjust the destination schema based on DDL events, you would do that here.
			case binlogdatapb.VEventType_DDL:
				err = v.flush(ctx)
				if err != nil {
					return err
				}

			// COPY_COMPLETED events come through once per keyspace/shard, and an additional fully completed event
			// as noted in go/vt/vtgate/endtoend/vstream_test.go:951.
			// "The arrival order of COPY_COMPLETED events with keyspace/shard is not constant.
			// On the other hand, the last event should always be a fully COPY_COMPLETED event."
			case binlogdatapb.VEventType_COPY_COMPLETED:
				if isFinalCopyCompletedEvent(ev) {
					err = v.flush(ctx)
					if err != nil {
						return err
					}

					err = setCopyCompleted(ctx, v.session, v.name, v.vgtidStateKeyspace, v.vgtidStateTable)
					if err != nil {
						return err
					}
				}

			// heartbeat events are sent periodically if the source keyspace is idle and there are no other events.
			// It's possible that there is still buffered data that hadn't exceeded the min duration or max rows
			// thresholds the last time flush was called. Most of the time, that won't be the case, but flush will
			// check for that and only run if necessary.
			case binlogdatapb.VEventType_HEARTBEAT:
				err = v.flush(ctx)
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
	}
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
// the heartbeat ticker will be used to ensure that we haven't been disconnected from the stream. This starts
// a goroutine that will cancel the context if we haven't received an event in twice the heartbeat duration.
// ********************************************************************************************************
func (v *VStreamClient) monitorHeartbeat(ctx context.Context) {
	// Start heartbeat liveness tracking when Run begins so startup stalls are treated the same
	// as post-start disconnects.
	v.lastEventReceivedAtUnixNano.Store(time.Now().UnixNano())

	const timeoutMultiplier = 2

	heartbeatDur := time.Duration(v.flags.HeartbeatInterval) * time.Second
	heartbeat := time.NewTicker(heartbeatDur)
	defer heartbeat.Stop()

	for {
		select {
		case tm := <-heartbeat.C:
			// if we haven't received an event in twice the heartbeat duration, we'll cancel the context, since
			// we're likely disconnected, and exit the goroutine
			if tm.Sub(time.Unix(0, v.lastEventReceivedAtUnixNano.Load())) > heartbeatDur*timeoutMultiplier {
				v.GracefulShutdown(v.gracefulShutdownWaitDur)
				return
			}

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
// ********************************************************************************************************
//
// we might consider exporting Flush, but we'd need to have a mutex or something to block the stream from
// processing, and technically we'd need to let it run until a commit event happens.
func (v *VStreamClient) flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("vstreamclient: context error before flush: %w", err)
	}

	hasBufferedRows := false
	for _, table := range v.tables {
		if len(table.currentBatch) > 0 {
			hasBufferedRows = true
			break
		}
	}

	// if the lastFlushedVgtid is the same as the latestVgtid and there is no buffered data,
	// we don't need to do anything.
	if !hasBufferedRows && proto.Equal(v.lastFlushedVgtid, v.latestVgtid) {
		if v.isClosing.Load() {
			v.gracefulShutdownFlushOnce.Do(func() {
				close(v.gracefulShutdownFlushChan)
			})
		}
		return nil
	}

	// if we have exceeded the minFlushDuration, we'll force a flush, regardless how many rows each table has
	shouldFlush := time.Since(v.stats.LastFlushedAt) > v.minFlushDuration

	// if we haven't exceeded the min flush duration, we'll check if any of the tables have exceeded their
	// max rows to flush. If any of them have, we will force a flush. Every table needs to be flushed at
	// the same time, since the last vgtid covers all tables.
	if !shouldFlush {
		for _, table := range v.tables {
			if len(table.currentBatch) >= table.MaxRowsPerFlush {
				shouldFlush = true
				break
			}
		}
	}

	// even if we haven't hit either of minDuration or maxRows, if we're actively closing
	// down, go ahead and force the flush.
	if !shouldFlush {
		shouldFlush = v.isClosing.Load()
	}

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
	err := updateLatestVGtid(ctx, v.session, v.name, v.vgtidStateKeyspace, v.vgtidStateTable, v.latestVgtid)
	if err != nil {
		return err
	}

	v.stats.FlushCount++
	v.stats.LastFlushedAt = time.Now()
	v.lastFlushedVgtid = v.latestVgtid

	// if we're in the middle of a graceful shutdown, and we've successfully flushed all the remaining data,
	// we can close the channel to let the Run loop know it can exit
	if v.isClosing.Load() {
		v.gracefulShutdownFlushOnce.Do(func() {
			close(v.gracefulShutdownFlushChan)
		})
	}

	return nil
}
