/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

/*
		This is a sample client for streaming using the vstream API. It is setup to work with the local example and you can
	    either stream from the unsharded commerce keyspace or the customer keyspace after the sharding step.
*/
func main() {
	ctx := context.Background()

	vgtid, err := getLastVgtid(ctx)
	if err != nil {
		log.Fatal(err)
	}

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  "customer",
			Filter: "select * from customer",
		}},
	}

	conn, err := vtgateconn.Dial(ctx, "localhost:15991")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	flags := &vtgatepb.VStreamFlags{
		// MinimizeSkew:      false,
		// HeartbeatInterval: 60, //seconds
		// StopOnReshard: true,
		// IncludeReshardJournalEvents: true,
	}
	reader, err := conn.VStream(ctx, topodatapb.TabletType_PRIMARY, vgtid, filter, flags)
	if err != nil {
		log.Fatal(err)
	}

	err = readEvents(ctx, reader, time.Duration(flags.HeartbeatInterval)*time.Second)
	if err != nil {
		log.Fatal(err)
	}
}

// getLastVgtid retrieves the last vgtid processed by the client, so that it can resume from that position.
func getLastVgtid(ctx context.Context) (*binlogdatapb.VGtid, error) {
	var vgtid binlogdatapb.VGtid

	// if storeLastVgtid was implemented, you would retrieve the last vgtid and unmarshal it here
	// err := json.Unmarshal([]byte{}, &vgtid)
	// if err != nil {
	// 	return nil, err
	// }

	streamCustomer := true
	if streamCustomer {
		vgtid = binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: "customer",
				Shard:    "-80",
				// Gtid "" is to stream from the start, "current" is to stream from the current gtid
				// you can also specify a gtid to start with.
				Gtid: "", // "current"  // "MySQL56/36a89abd-978f-11eb-b312-04ed332e05c2:1-265"
			}, {
				Keyspace: "customer",
				Shard:    "80-",
				Gtid:     "",
			}},
		}
	} else {
		vgtid = binlogdatapb.VGtid{
			ShardGtids: []*binlogdatapb.ShardGtid{{
				Keyspace: "commerce",
				Shard:    "0",
				Gtid:     "",
			}},
		}
	}

	return &vgtid, nil
}

// storeLastVgtid stores the last vgtid processed by the client, so that it can resume from that position on restart.
// Storing a json blob in a database is just one way to do this, you could put it anywhere.
func storeLastVgtid(ctx context.Context, vgtid *binlogdatapb.VGtid) error {
	_, err := json.Marshal(vgtid)
	if err != nil {
		return err
	}

	return nil
}

// Customer is the concrete type that will be built from the stream
type Customer struct {
	ID    int64
	Email string

	// the fields below aren't actually in the schema, but are added for illustrative purposes
	EmailConfirmed bool
	Details        map[string]any
	CreatedAt      time.Time
}

// to avoid flushing too often, we will only flush if it has been at least minFlushDuration since the last flush.
// we're relying on heartbeat events to handle max duration between flushes, in case there are no other events.
const minFlushDuration = 5 * time.Second

// maxCustomersToFlush serves two purposes:
//  1. it limits the number of customers that are flushed at once, to avoid large transactions. If more than this number
//     of customers are processed, they will be flushed in chunks of this size.
//  2. if this number is exceeded before reaching the minFlushDuration, it will trigger a flush to avoid holding too much
//     memory in the customers slice.
const maxCustomersToFlush = 1000

func readEvents(ctx context.Context, reader vtgateconn.VStreamReader, heartbeatDur time.Duration) error {
	// customers built from the stream will be stored in this slice, until it's time to flush them to the database,
	// at which point they will be cleared and the slice reused
	customers := make([]*Customer, 0, maxCustomersToFlush)

	// the first events will be field events, which contains schema information for any tables that are being streamed
	var customerFields, corderFields []*querypb.Field

	// ********************************************************************************************************
	// Heartbeat Monitoring
	//
	// the heartbeat ticker will be used to ensure that we haven't been disconnected from the stream. This starts
	// a goroutine that will cancel the context if we haven't received an event in twice the heartbeat duration.
	// We purposefully overwrite the ctx variable to ensure that we don't accidentally use the original context.
	// ********************************************************************************************************
	var lastEventReceivedAtUnix atomic.Int64

	ctx, heartbeatCancel := context.WithCancel(ctx)
	if heartbeatDur == 0 {
		heartbeatDur = 30 * time.Second
	}
	heartbeat := time.NewTicker(heartbeatDur)
	defer heartbeat.Stop()

	go func() {
		for {
			select {
			case tm := <-heartbeat.C:
				// if we haven't received an event yet, we'll skip the heartbeat check
				if lastEventReceivedAtUnix.Load() == 0 {
					continue
				}

				// if we haven't received an event in twice the heartbeat duration, we'll cancel the context, since
				// we're likely disconnected, and exit the goroutine
				if tm.Sub(time.Unix(lastEventReceivedAtUnix.Load(), 0)) > heartbeatDur*2 {
					heartbeatCancel()
					return
				}

			case <-ctx.Done():
				// since heartbeatCtx is derived from ctx, there's no need to cancel it
				return
			}
		}
	}()

	// ********************************************************************************************************
	// Flush Data + Store Vgtid
	//
	// as we process events, we'll periodically need to check point the last vgtid and store the customers in the
	// database. You can control the frequency of this flush by adjusting the minFlushDuration and maxCustomersToFlush.
	// This is only called when we have an event that guarantees we're not flushing mid-transaction.
	// ********************************************************************************************************
	var lastFlushedVgtid, latestVgtid *binlogdatapb.VGtid
	var lastFlushedAt time.Time

	flushFunc := func() error {
		// if the lastFlushedVgtid is the same as the latestVgtid, we don't need to do anything
		if proto.Equal(lastFlushedVgtid, latestVgtid) {
			return nil
		}

		// if it hasn't been long enough since the last flush, and we haven't exceeded our match batch size, don't
		// flush. We can always replay as needed.
		if time.Since(lastFlushedAt) < minFlushDuration && len(customers) < maxCustomersToFlush {
			return nil
		}

		// if the customer db is the same db you're storing the vgtid in, you could do both in the same transaction

		// flush the customers to the database, using the max batch size
		for customerChunk := range slices.Chunk(customers, maxCustomersToFlush) {
			err := upsertCustomersToDB(ctx, customerChunk)
			if err != nil {
				return err
			}
		}

		// reset the customers slice to free up memory. If you really want to be efficient, you could reuse the slice.
		customers = slices.Delete(customers, 0, len(customers))

		// always store the latest vgtid, even if there are no customers to store
		err := storeLastVgtid(ctx, latestVgtid)
		if err != nil {
			return err
		}

		lastFlushedVgtid = latestVgtid
		lastFlushedAt = time.Now()

		return nil
	}

	// ********************************************************************************************************
	// Event Processing
	//
	// this is the main loop that processes events from the stream. It will continue until the stream ends or an
	// error occurs. The context is checked before processing each event.
	// ********************************************************************************************************
	for {
		// events come in batches, depending on how busy the keyspace is. This is where the network communication
		// happens, so it's the most likely place for errors to occur.
		events, err := reader.Recv()
		switch {
		case err == nil: // no error, continue processing below

		case errors.Is(err, io.EOF):
			fmt.Println("stream ended")
			return nil

		default:
			return fmt.Errorf("remote error: %w", err)
		}

		for _, ev := range events {
			// check for context errors before processing the next event, since any processing will likely be wasted
			err = ctx.Err()
			if err != nil {
				return fmt.Errorf("context error: %w", err)
			}

			// keep track of the last event time for heartbeat monitoring. We're purposefully not using the event
			// timestamp, since that would cause cancellation if the stream was copying, delayed, or lagging.
			lastEventReceivedAtUnix.Store(time.Now().Unix())

			// handle individual events based on their type
			switch ev.Type {
			// field events are sent first, and contain schema information for any tables that are being streamed,
			// so we cache the fields for each table as they come in
			case binlogdatapb.VEventType_FIELD:
				switch strings.TrimPrefix(ev.FieldEvent.TableName, ev.Keyspace+".") {
				case "customer":
					customerFields = ev.FieldEvent.Fields

				case "corder":
					corderFields = ev.FieldEvent.Fields

				default:
					return errors.New("unexpected table name: " + ev.RowEvent.TableName)
				}

			// row events are the actual data changes, and we'll process them based on the table name
			case binlogdatapb.VEventType_ROW:
				// since our filter is "select * from customer", we could rely on that and not check the table name,
				// but this shows how you might handle multiple tables in the same stream
				switch strings.TrimPrefix(ev.RowEvent.TableName, ev.Keyspace+".") {
				case "customer":
					var customer *Customer
					customer, err = processCustomerRowEvent(customerFields, ev.RowEvent)
					if err != nil {
						return err
					}

					// we're not handling deletes, so we'll ignore nil customer return values
					if customer != nil {
						customers = append(customers, customer)
					}

				case "corder":
					fmt.Printf("corder event: %v | fields: %v\n", ev.RowEvent, corderFields)
					return errors.New("corder support not implemented")
				}

			// vgtid events are sent periodically, and we'll store the latest vgtid for checkpointing. We may get
			// this mid-transaction, so we don't flush here, so we don't propagate a partial transaction that may
			// be rolled back.
			case binlogdatapb.VEventType_VGTID:
				latestVgtid = ev.Vgtid

			// commit and other events are safe to flush on, since they indicate the end of a transaction.
			// Otherwise, there's not much to do with these events.
			case binlogdatapb.VEventType_COMMIT, binlogdatapb.VEventType_OTHER:
				// only flush when we have an event that guarantees we're not flushing mid-transaction
				err = flushFunc()
				if err != nil {
					return err
				}

			// DDL events are schema changes, and we might want to handle them differently than data events.
			// They are safe to flush on, since they indicate the end of a transaction. If you want to
			// transparently adjust the destination schema based on DDL events, you would do that here.
			case binlogdatapb.VEventType_DDL:
				err = flushFunc()
				if err != nil {
					return err
				}

				// alter the destination schema based on the DDL event

			case binlogdatapb.VEventType_COPY_COMPLETED:
				// TODO: don't flush until the copy is completed? do some sort of cleanup if we haven't received this?

			// we don't need to do anything with these events, but the timestamp from these and other events
			// is recorded at the top of the loop for heartbeat monitoring
			case binlogdatapb.VEventType_HEARTBEAT:

			// even if there are no changes to the tables being streamed, we'll still get a begin, vgtid, and commit
			// event for each transaction. The other two are used for checkpoints, but nothing to do here.
			case binlogdatapb.VEventType_BEGIN:

			// journal events are sent on resharding. Unless you are manually targeting shards, vstream should
			// transparently handle resharding for you, so you shouldn't need to do anything with these events.
			// You might want to log them for debugging purposes, or to alert on resharding events in case
			// something goes wrong. After resharding, if the pre-reshard vgtid is no longer valid, you may need
			// to restart the stream from the beginning.
			case binlogdatapb.VEventType_JOURNAL:

			// there aren't strong cases for handling these events, but you might want to log them for debugging
			case binlogdatapb.VEventType_VERSION, binlogdatapb.VEventType_LASTPK, binlogdatapb.VEventType_SAVEPOINT:
			}
		}
	}
}

// processCustomerRowEvent builds a customer from a row event. It will only process the last row in the event.
// If you wanted to process all rows in the event, you would need to change the function signature to return a slice.
func processCustomerRowEvent(fields []*querypb.Field, rowEvent *binlogdatapb.RowEvent) (*Customer, error) {
	if fields == nil {
		return nil, errors.New("internal error: unexpected rows without fields")
	}

	var customer *Customer
	var err error

	// TODO: I'm not exactly sure how to handle multiple rows in a single event, so I'm just going to take the last one
	for _, rc := range rowEvent.RowChanges {
		// ignore deletes
		if rc.After == nil {
			continue
		}

		row := sqltypes.MakeRowTrusted(fields, rc.After)

		customer, err = rowToCustomer(fields, row)
		if err != nil {
			return nil, err
		}
	}

	return customer, nil
}

// rowToCustomer builds a customer from a row event
func rowToCustomer(fields []*querypb.Field, row []sqltypes.Value) (*Customer, error) {
	customer := &Customer{}
	var err error

	for i := range row {
		if row[i].IsNull() {
			continue
		}

		switch fields[i].Name {
		case "workspace_id":
			customer.ID, err = row[i].ToCastInt64()

		case "email":
			customer.Email = row[i].ToString()

		// the fields below aren't actually in the example schema, but are added to
		// show how you should handle different data types

		case "email_confirmed":
			customer.EmailConfirmed, err = row[i].ToBool()

		case "details":
			// assume the details field is a json blob
			var b []byte
			b, err = row[i].ToBytes()
			if err == nil {
				err = json.Unmarshal(b, &customer.Details)
			}

		case "created_at":
			customer.CreatedAt, err = row[i].ToTime()
		}
		if err != nil {
			return nil, fmt.Errorf("error processing field %s: %w", fields[i].Name, err)
		}
	}

	return customer, nil
}

// upsertCustomersToDB is a placeholder for the function that would actually store the customers in the database,
// sync the data to another system, etc.
func upsertCustomersToDB(ctx context.Context, customers []*Customer) error {
	fmt.Printf("upserting %d customers\n", len(customers))
	for i, customer := range customers {
		fmt.Printf("upserting customer %d: %v\n", i, customer)
	}
	return nil
}
