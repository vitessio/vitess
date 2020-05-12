/*
Copyright 2019 The Vitess Authors.

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

package vtgate

import (
	"fmt"
	"io"
	"sync"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
)

// vstreamManager manages vstream requests.
type vstreamManager struct {
	resolver *srvtopo.Resolver
	toposerv srvtopo.Server
	cell     string
}

// vstream contains the metadata for one VStream request.
type vstream struct {
	// mu protects parts of vgtid, the semantics of a send, and journaler.
	// Once streaming begins, the Gtid within each ShardGtid will be updated on each event.
	// Also, the list of ShardGtids can change on a journaling event.
	// All other parts of vgtid can be read without a lock.
	// The lock is also held to ensure that all grouped events are sent together.
	// This can happen if vstreamer breaks up large transactions into smaller chunks.
	mu        sync.Mutex
	vgtid     *binlogdatapb.VGtid
	send      func(events []*binlogdatapb.VEvent) error
	journaler map[int64]*journalEvent

	// err can only be set once.
	once sync.Once
	err  error

	// Other input parameters
	tabletType topodatapb.TabletType
	filter     *binlogdatapb.Filter
	resolver   *srvtopo.Resolver

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type journalEvent struct {
	journal      *binlogdatapb.Journal
	participants map[*binlogdatapb.ShardGtid]bool
	done         chan struct{}
}

func newVStreamManager(resolver *srvtopo.Resolver, serv srvtopo.Server, cell string) *vstreamManager {
	return &vstreamManager{
		resolver: resolver,
		toposerv: serv,
		cell:     cell,
	}
}

func (vsm *vstreamManager) VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter, send func(events []*binlogdatapb.VEvent) error) error {
	vgtid, filter, err := vsm.resolveParams(ctx, tabletType, vgtid, filter)
	if err != nil {
		return err
	}
	vs := &vstream{
		vgtid:      vgtid,
		tabletType: tabletType,
		filter:     filter,
		send:       send,
		resolver:   vsm.resolver,
		journaler:  make(map[int64]*journalEvent),
	}
	return vs.stream(ctx)
}

// resolveParams provides defaults for the inputs if they're not specified.
func (vsm *vstreamManager) resolveParams(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid, filter *binlogdatapb.Filter) (*binlogdatapb.VGtid, *binlogdatapb.Filter, error) {
	if filter == nil {
		filter = &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match: "/.*",
			}},
		}
	}
	if vgtid == nil || len(vgtid.ShardGtids) == 0 {
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "vgtid must have at least one value with a starting position")
	}
	// To fetch from all keyspaces, the input must contain a single ShardGtid
	// that has an empty keyspace, and the Gtid must be "current". In the
	// future, we'll allow the Gtid to be empty which will also support
	// copying of existing data.
	if len(vgtid.ShardGtids) == 1 && vgtid.ShardGtids[0].Keyspace == "" {
		if vgtid.ShardGtids[0].Gtid != "current" {
			return nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "for an empty keyspace, the Gtid value must be 'current': %v", vgtid)
		}
		keyspaces, err := vsm.toposerv.GetSrvKeyspaceNames(ctx, vsm.cell, false)
		if err != nil {
			return nil, nil, err
		}
		newvgtid := &binlogdatapb.VGtid{}
		for _, keyspace := range keyspaces {
			newvgtid.ShardGtids = append(newvgtid.ShardGtids, &binlogdatapb.ShardGtid{
				Keyspace: keyspace,
				Gtid:     "current",
			})
		}
		vgtid = newvgtid
	}
	newvgtid := &binlogdatapb.VGtid{}
	for _, sgtid := range vgtid.ShardGtids {
		if sgtid.Shard == "" {
			if sgtid.Gtid != "current" {
				return nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "if shards are unspecified, the Gtid value must be 'current': %v", vgtid)
			}
			// TODO(sougou): this should work with the new Migrate workflow
			_, _, allShards, err := vsm.resolver.GetKeyspaceShards(ctx, sgtid.Keyspace, tabletType)
			if err != nil {
				return nil, nil, err
			}
			for _, shard := range allShards {
				newvgtid.ShardGtids = append(newvgtid.ShardGtids, &binlogdatapb.ShardGtid{
					Keyspace: sgtid.Keyspace,
					Shard:    shard.Name,
					Gtid:     sgtid.Gtid,
				})
			}
		} else {
			newvgtid.ShardGtids = append(newvgtid.ShardGtids, sgtid)
		}
	}
	return newvgtid, filter, nil
}

func (vs *vstream) stream(ctx context.Context) error {
	ctx, vs.cancel = context.WithCancel(ctx)
	defer vs.cancel()

	// Make a copy first, because the ShardGtids list can change once streaming starts.
	copylist := append(([]*binlogdatapb.ShardGtid)(nil), vs.vgtid.ShardGtids...)
	for _, sgtid := range copylist {
		vs.startOneStream(ctx, sgtid)
	}
	vs.wg.Wait()
	return vs.err
}

// startOneStream sets up one shard stream.
func (vs *vstream) startOneStream(ctx context.Context, sgtid *binlogdatapb.ShardGtid) {
	vs.wg.Add(1)
	go func() {
		defer vs.wg.Done()
		err := vs.streamFromTablet(ctx, sgtid)

		// Set the error on exit. First one wins.
		if err != nil {
			vs.once.Do(func() {
				vs.err = err
				vs.cancel()
			})
		}
	}()
}

// streamFromTablet streams from one shard. If transactions come in separate chunks, they are grouped and sent.
func (vs *vstream) streamFromTablet(ctx context.Context, sgtid *binlogdatapb.ShardGtid) error {
	// journalDone is assigned a channel when a journal event is encountered.
	// It will be closed when all journal events converge.
	var journalDone chan struct{}

	errCount := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-journalDone:
			// Unreachable.
			// This can happen if a server misbehaves and does not end
			// the stream after we return an error.
			return nil
		default:
		}

		var eventss [][]*binlogdatapb.VEvent
		rss, err := vs.resolver.ResolveDestination(ctx, sgtid.Keyspace, vs.tabletType, key.DestinationShard(sgtid.Shard))
		if err != nil {
			return err
		}
		if len(rss) != 1 {
			// Unreachable.
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unexpected number or shards: %v", rss)
		}
		// Safe to access sgtid.Gtid here (because it can't change until streaming begins).
		err = rss[0].QueryService.VStream(ctx, rss[0].Target, sgtid.Gtid, vs.filter, func(events []*binlogdatapb.VEvent) error {
			// We received a valid event. Reset error count.
			errCount = 0

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-journalDone:
				// Unreachable.
				// This can happen if a server misbehaves and does not end
				// the stream after we return an error.
				return io.EOF
			default:
			}

			sendevents := make([]*binlogdatapb.VEvent, 0, len(events))
			for _, event := range events {
				switch event.Type {
				case binlogdatapb.VEventType_FIELD:
					// Update table names and send.
					// If we're streaming from multiple keyspaces, this will disambiguate
					// duplicate table names.
					ev := proto.Clone(event).(*binlogdatapb.VEvent)
					ev.FieldEvent.TableName = sgtid.Keyspace + "." + ev.FieldEvent.TableName
					sendevents = append(sendevents, ev)
				case binlogdatapb.VEventType_ROW:
					// Update table names and send.
					ev := proto.Clone(event).(*binlogdatapb.VEvent)
					ev.RowEvent.TableName = sgtid.Keyspace + "." + ev.RowEvent.TableName
					sendevents = append(sendevents, ev)
				case binlogdatapb.VEventType_COMMIT, binlogdatapb.VEventType_DDL:
					sendevents = append(sendevents, event)
					eventss = append(eventss, sendevents)
					if err := vs.sendAll(sgtid, eventss); err != nil {
						return err
					}
					eventss = nil
					sendevents = nil
				case binlogdatapb.VEventType_HEARTBEAT:
					// Remove all heartbeat events for now.
					// Otherwise they can accumulate indefinitely if there are no real events.
					// TODO(sougou): figure out a model for this.
				case binlogdatapb.VEventType_JOURNAL:
					journal := event.Journal
					// Journal events are not sent to clients.
					je, err := vs.getJournalEvent(ctx, sgtid, journal)
					if err != nil {
						return err
					}
					if je != nil {
						// Wait till all other participants converge and return EOF.
						journalDone = je.done
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-journalDone:
							return io.EOF
						}
					}
				default:
					sendevents = append(sendevents, event)
				}
			}
			if len(sendevents) != 0 {
				eventss = append(eventss, sendevents)
			}
			return nil
		})
		// If stream was ended (by a journal event), return nil without checking for error.
		select {
		case <-journalDone:
			return nil
		default:
		}
		if err == nil {
			// Unreachable.
			err = vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "vstream ended unexpectedly")
		}
		if vterrors.Code(err) != vtrpcpb.Code_FAILED_PRECONDITION && vterrors.Code(err) != vtrpcpb.Code_UNAVAILABLE {
			log.Errorf("vstream for %s/%s error: %v", sgtid.Keyspace, sgtid.Shard, err)
			return err
		}
		errCount++
		if errCount >= 3 {
			log.Errorf("vstream for %s/%s had three consecutive failures: %v", sgtid.Keyspace, sgtid.Shard, err)
			return err
		}
		log.Infof("vstream for %s/%s error, retrying: %v", sgtid.Keyspace, sgtid.Shard, err)
	}
}

// sendAll sends a group of events together while holding the lock.
func (vs *vstream) sendAll(sgtid *binlogdatapb.ShardGtid, eventss [][]*binlogdatapb.VEvent) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Send all chunks while holding the lock.
	for _, events := range eventss {
		// convert all gtids to vgtids. This should be done here while holding the lock.
		for j, event := range events {
			if event.Type == binlogdatapb.VEventType_GTID {
				// Update the VGtid and send that instead.
				sgtid.Gtid = event.Gtid
				events[j] = &binlogdatapb.VEvent{
					Type:  binlogdatapb.VEventType_VGTID,
					Vgtid: proto.Clone(vs.vgtid).(*binlogdatapb.VGtid),
				}
			}
		}
		if err := vs.send(events); err != nil {
			return err
		}
	}
	return nil
}

// getJournalEvent returns a journalEvent. The caller has to wait on its done channel.
// Once it closes, the caller has to return (end their stream).
// The function has three parts:
// Part 1: For the first stream that encounters an event, it creates a journal event.
// Part 2: Every stream joins the journalEvent. If all have not joined, the journalEvent
// is returned to the caller.
// Part 3: If all streams have joined, then new streams are created to replace existing
// streams, the done channel is closed and returned. This section is executed exactly
// once after the last stream joins.
func (vs *vstream) getJournalEvent(ctx context.Context, sgtid *binlogdatapb.ShardGtid, journal *binlogdatapb.Journal) (*journalEvent, error) {
	if journal.MigrationType == binlogdatapb.MigrationType_TABLES {
		// We cannot support table migrations yet because there is no
		// good model for it yet. For example, what if a table is migrated
		// out of the current keyspace we're streaming from.
		return nil, nil
	}

	vs.mu.Lock()
	defer vs.mu.Unlock()

	je, ok := vs.journaler[journal.Id]
	if !ok {
		log.Infof("Journal encountered: %v", journal)
		// Identify the list of ShardGtids that match the participants of the journal.
		je = &journalEvent{
			journal:      journal,
			participants: make(map[*binlogdatapb.ShardGtid]bool),
			done:         make(chan struct{}),
		}
		const (
			undecided = iota
			matchAll
			matchNone
		)
		// We start off as undecided. Once we transition to
		// matchAll or matchNone, we have to stay in that state.
		mode := undecided
	nextParticipant:
		for _, jks := range journal.Participants {
			for _, inner := range vs.vgtid.ShardGtids {
				if inner.Keyspace == jks.Keyspace && inner.Shard == jks.Shard {
					switch mode {
					case undecided, matchAll:
						mode = matchAll
						je.participants[inner] = false
					case matchNone:
						return nil, fmt.Errorf("not all journaling participants are in the stream: journal: %v, stream: %v", journal.Participants, vs.vgtid.ShardGtids)
					}
					continue nextParticipant
				}
			}
			switch mode {
			case undecided, matchNone:
				mode = matchNone
			case matchAll:
				return nil, fmt.Errorf("not all journaling participants are in the stream: journal: %v, stream: %v", journal.Participants, vs.vgtid.ShardGtids)
			}
		}
		if mode == matchNone {
			// Unreachable. Journal events are only added to participants.
			// But if we do receive such an event, the right action will be to ignore it.
			return nil, nil
		}
		vs.journaler[journal.Id] = je
	}

	if _, ok := je.participants[sgtid]; !ok {
		// Unreachable. See above.
		return nil, nil
	}
	je.participants[sgtid] = true

	for _, waiting := range je.participants {
		if !waiting {
			// Some participants are yet to join the wait.
			return je, nil
		}
	}
	// All participants are waiting. Replace old shard gtids with new ones.
	newsgtids := make([]*binlogdatapb.ShardGtid, 0, len(vs.vgtid.ShardGtids)-len(je.participants)+len(je.journal.ShardGtids))
	log.Infof("Removing shard gtids: %v", je.participants)
	for _, cursgtid := range vs.vgtid.ShardGtids {
		if je.participants[cursgtid] {
			continue
		}
		newsgtids = append(newsgtids, cursgtid)
	}

	log.Infof("Adding shard gtids: %v", je.journal.ShardGtids)
	for _, sgtid := range je.journal.ShardGtids {
		newsgtids = append(newsgtids, sgtid)
		// It's ok to start the streams eventhough ShardGtids is not updated yet.
		// This is because we're still holding the lock.
		vs.startOneStream(ctx, sgtid)
	}
	vs.vgtid.ShardGtids = newsgtids
	close(je.done)
	return je, nil
}
