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
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// vstreamManager manages vstream requests.
type vstreamManager struct {
	resolver *srvtopo.Resolver
	toposerv srvtopo.Server
	cell     string

	vstreamsCreated         *stats.CountersWithMultiLabels
	vstreamsLag             *stats.GaugesWithMultiLabels
	vstreamsCount           *stats.CountersWithMultiLabels
	vstreamsEventsStreamed  *stats.CountersWithMultiLabels
	vstreamsEndedWithErrors *stats.CountersWithMultiLabels
}

// maxSkewTimeoutSeconds is the maximum allowed skew between two streams when the MinimizeSkew flag is set
const maxSkewTimeoutSeconds = 10 * 60

// tabletPickerContextTimeout is the timeout for the child context used to select candidate tablets
// for a vstream
const tabletPickerContextTimeout = 90 * time.Second

// stopOnReshardDelay is how long we wait, at a minimum, after sending a reshard journal event before
// ending the stream from the tablet.
const stopOnReshardDelay = 500 * time.Millisecond

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
	// errMu protects err by ensuring its value is read or written by only one goroutine at a time.
	once  sync.Once
	err   error
	errMu sync.Mutex

	// Other input parameters
	tabletType topodatapb.TabletType
	filter     *binlogdatapb.Filter
	resolver   *srvtopo.Resolver
	optCells   string

	cancel context.CancelFunc
	wg     sync.WaitGroup

	// this flag is set by the client, default false
	// if true skew detection is enabled and we align the streams so that they receive events from
	// about the same time as each other. Note that there is no exact ordering of events across shards
	minimizeSkew bool

	// this flag is set by the client, default false
	// if true when a reshard is detected the client will send the corresponding journal event to the client
	// default behavior is to automatically migrate the resharded streams from the old to the new shards
	stopOnReshard bool

	// This flag is set by the client, default is false.
	// If true then the reshard journal events are sent in the stream irrespective of the stopOnReshard flag.
	includeReshardJournalEvents bool

	// mutex used to synchronize access to skew detection parameters
	skewMu sync.Mutex
	// channel is created whenever there is a skew detected. closing it implies the current skew has been fixed
	skewCh chan bool
	// if a skew lasts for this long, we timeout the vstream call. currently hardcoded
	skewTimeoutSeconds int64
	// the slow streamId which is causing the skew. streamId is of the form <keyspace>.<shard>
	laggard string
	// transaction timestamp of the slowest stream
	lowestTS int64
	// the timestamp of the most recent event, keyed by streamId. streamId is of the form <keyspace>.<shard>
	timestamps map[string]int64

	// the shard map tracking the copy completion, keyed by streamId. streamId is of the form <keyspace>.<shard>
	copyCompletedShard map[string]struct{}

	vsm *vstreamManager

	eventCh           chan []*binlogdatapb.VEvent
	heartbeatInterval uint32
	ts                *topo.Server

	tabletPickerOptions discovery.TabletPickerOptions

	flags *vtgatepb.VStreamFlags
}

type journalEvent struct {
	journal      *binlogdatapb.Journal
	participants map[*binlogdatapb.ShardGtid]bool
	done         chan struct{}
}

func newVStreamManager(resolver *srvtopo.Resolver, serv srvtopo.Server, cell string) *vstreamManager {
	exporter := servenv.NewExporter(cell, "VStreamManager")
	labels := []string{"Keyspace", "ShardName", "TabletType"}

	return &vstreamManager{
		resolver: resolver,
		toposerv: serv,
		cell:     cell,
		vstreamsCreated: exporter.NewCountersWithMultiLabels(
			"VStreamsCreated",
			"Number of vstreams created",
			labels),
		vstreamsLag: exporter.NewGaugesWithMultiLabels(
			"VStreamsLag",
			"Difference between event current time and the binlog event timestamp",
			labels),
		vstreamsCount: exporter.NewCountersWithMultiLabels(
			"VStreamsCount",
			"Number of active vstreams",
			labels),
		vstreamsEventsStreamed: exporter.NewCountersWithMultiLabels(
			"VStreamsEventsStreamed",
			"Number of events sent across all vstreams",
			labels),
		vstreamsEndedWithErrors: exporter.NewCountersWithMultiLabels(
			"VStreamsEndedWithErrors",
			"Number of vstreams that ended with errors",
			labels),
	}
}

func (vsm *vstreamManager) VStream(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid,
	filter *binlogdatapb.Filter, flags *vtgatepb.VStreamFlags, send func(events []*binlogdatapb.VEvent) error) error {
	vgtid, filter, flags, err := vsm.resolveParams(ctx, tabletType, vgtid, filter, flags)
	if err != nil {
		return vterrors.Wrap(err, "failed to resolve vstream parameters")
	}
	ts, err := vsm.toposerv.GetTopoServer()
	if err != nil {
		return vterrors.Wrap(err, "failed to get topology server")
	}
	if ts == nil {
		log.Errorf("unable to get topo server in VStream()")
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unable to get topoology server")
	}
	vs := &vstream{
		vgtid:                       vgtid,
		tabletType:                  tabletType,
		optCells:                    flags.Cells,
		filter:                      filter,
		send:                        send,
		resolver:                    vsm.resolver,
		journaler:                   make(map[int64]*journalEvent),
		minimizeSkew:                flags.GetMinimizeSkew(),
		stopOnReshard:               flags.GetStopOnReshard(),
		includeReshardJournalEvents: flags.GetIncludeReshardJournalEvents(),
		skewTimeoutSeconds:          maxSkewTimeoutSeconds,
		timestamps:                  make(map[string]int64),
		vsm:                         vsm,
		eventCh:                     make(chan []*binlogdatapb.VEvent),
		heartbeatInterval:           flags.GetHeartbeatInterval(),
		ts:                          ts,
		copyCompletedShard:          make(map[string]struct{}),
		tabletPickerOptions: discovery.TabletPickerOptions{
			CellPreference: flags.GetCellPreference(),
			TabletOrder:    flags.GetTabletOrder(),
			// This is NOT configurable via the API because we check the
			// discovery.GetLowReplicationLag().Seconds() value in the tablet
			// health stream.
			ExcludeTabletsWithMaxReplicationLag: discovery.GetLowReplicationLag(),
		},
		flags: flags,
	}
	return vs.stream(ctx)
}

// resolveParams provides defaults for the inputs if they're not specified.
func (vsm *vstreamManager) resolveParams(ctx context.Context, tabletType topodatapb.TabletType, vgtid *binlogdatapb.VGtid,
	filter *binlogdatapb.Filter, flags *vtgatepb.VStreamFlags) (*binlogdatapb.VGtid, *binlogdatapb.Filter, *vtgatepb.VStreamFlags, error) {

	if filter == nil {
		filter = &binlogdatapb.Filter{
			Rules: []*binlogdatapb.Rule{{
				Match: "/.*",
			}},
		}
	}

	if flags == nil {
		flags = &vtgatepb.VStreamFlags{}
	}
	if vgtid == nil || len(vgtid.ShardGtids) == 0 {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "vgtid must have at least one value with a starting position in ShardGtids")
	}
	// To fetch from all keyspaces, the input must contain a single ShardGtid
	// that has an empty keyspace, and the Gtid must be "current".
	// Or the input must contain a single ShardGtid that has keyspace wildcards.
	if len(vgtid.ShardGtids) == 1 {
		inputKeyspace := vgtid.ShardGtids[0].Keyspace
		isEmpty := inputKeyspace == ""
		isRegexp := strings.HasPrefix(inputKeyspace, "/")
		if isEmpty || isRegexp {
			newvgtid := &binlogdatapb.VGtid{}
			keyspaces, err := vsm.toposerv.GetSrvKeyspaceNames(ctx, vsm.cell, false)
			if err != nil {
				return nil, nil, nil, vterrors.Wrapf(err, "failed to get keyspace names in cell %s", vsm.cell)
			}

			if isEmpty {
				if vgtid.ShardGtids[0].Gtid != "current" {
					return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "for an empty keyspace, the Gtid value must be 'current': %v", vgtid)
				}
				for _, keyspace := range keyspaces {
					newvgtid.ShardGtids = append(newvgtid.ShardGtids, &binlogdatapb.ShardGtid{
						Keyspace: keyspace,
						Gtid:     "current",
					})
				}
			} else {
				re, err := regexp.Compile(strings.Trim(inputKeyspace, "/"))
				if err != nil {
					return nil, nil, nil, vterrors.Wrapf(err, "failed to compile regexp using %s", inputKeyspace)
				}
				for _, keyspace := range keyspaces {
					if re.MatchString(keyspace) {
						newvgtid.ShardGtids = append(newvgtid.ShardGtids, &binlogdatapb.ShardGtid{
							Keyspace: keyspace,
							Gtid:     vgtid.ShardGtids[0].Gtid,
						})
					}
				}
			}
			vgtid = newvgtid
		}
	}
	newvgtid := &binlogdatapb.VGtid{}
	for _, sgtid := range vgtid.ShardGtids {
		if sgtid.Shard == "" {
			if sgtid.Gtid != "current" && sgtid.Gtid != "" {
				return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "if shards are unspecified, the Gtid value must be 'current' or empty; got: %+v",
					vgtid)
			}
			// TODO(sougou): this should work with the new Migrate workflow
			_, _, allShards, err := vsm.resolver.GetKeyspaceShards(ctx, sgtid.Keyspace, tabletType)
			if err != nil {
				return nil, nil, nil, vterrors.Wrapf(err, "failed to get shards in keyspace %s", sgtid.Keyspace)
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

	// TODO add tablepk validations

	return newvgtid, filter, flags, nil
}

func (vsm *vstreamManager) RecordStreamDelay() {
	vstreamSkewDelayCount.Add(1)
}

func (vsm *vstreamManager) GetTotalStreamDelay() int64 {
	return vstreamSkewDelayCount.Get()
}

func (vs *vstream) stream(ctx context.Context) error {
	ctx, vs.cancel = context.WithCancel(ctx)

	vs.wg.Add(1)
	go func() {
		defer vs.wg.Done()

		// sendEvents returns either if the given context has been canceled or if
		// an error is returned from the callback. If the callback returns an error,
		// we need to cancel the context to stop the other stream goroutines
		// and to unblock the VStream call.
		defer vs.cancel()

		vs.sendEvents(ctx)
	}()

	// Make a copy first, because the ShardGtids list can change once streaming starts.
	copylist := append(([]*binlogdatapb.ShardGtid)(nil), vs.vgtid.ShardGtids...)
	for _, sgtid := range copylist {
		vs.startOneStream(ctx, sgtid)
	}
	vs.wg.Wait()

	return vs.getError()
}

func (vs *vstream) sendEvents(ctx context.Context) {
	var heartbeat <-chan time.Time
	var resetHeartbeat func()

	if vs.heartbeatInterval == 0 {
		heartbeat = make(chan time.Time)
		resetHeartbeat = func() {}
	} else {
		d := time.Duration(vs.heartbeatInterval) * time.Second
		timer := time.NewTicker(d)
		defer timer.Stop()

		heartbeat = timer.C
		resetHeartbeat = func() { timer.Reset(d) }
	}

	send := func(evs []*binlogdatapb.VEvent) error {
		if err := vs.send(evs); err != nil {
			log.Infof("Error in vstream send (wrapper) to client: %v", err)
			vs.once.Do(func() {
				vs.setError(err, "error sending events")
			})
			return vterrors.Wrap(err, "error sending events")
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			log.Infof("vstream context canceled")
			vs.once.Do(func() {
				vs.setError(ctx.Err(), "context ended while sending events")
			})
			return
		case evs := <-vs.eventCh:
			if err := send(evs); err != nil {
				log.Infof("Error in vstream send events to client: %v", err)
				vs.once.Do(func() {
					vs.setError(err, "error sending events")
				})
				return
			}
			resetHeartbeat()
		case t := <-heartbeat:
			now := t.UnixNano()
			evs := []*binlogdatapb.VEvent{{
				Type:        binlogdatapb.VEventType_HEARTBEAT,
				Timestamp:   now / 1e9,
				CurrentTime: now,
			}}
			if err := send(evs); err != nil {
				log.Infof("Error in vstream sending heartbeat to client: %v", err)
				vs.once.Do(func() {
					vs.setError(err, "error sending heartbeat")
				})
				return
			}
		}
	}
}

// startOneStream sets up one shard stream.
func (vs *vstream) startOneStream(ctx context.Context, sgtid *binlogdatapb.ShardGtid) {
	vs.wg.Add(1)
	go func() {
		defer vs.wg.Done()

		labelValues := []string{sgtid.Keyspace, sgtid.Shard, vs.tabletType.String()}
		// Initialize vstreamsEndedWithErrors metric to zero.
		vs.vsm.vstreamsEndedWithErrors.Add(labelValues, 0)
		vs.vsm.vstreamsCreated.Add(labelValues, 1)
		vs.vsm.vstreamsCount.Add(labelValues, 1)

		err := vs.streamFromTablet(ctx, sgtid)

		// Set the error on exit. First one wins.
		if err != nil {
			log.Errorf("Error in vstream for %+v: %v", sgtid, err)
			// Get the original/base error.
			uerr := vterrors.UnwrapAll(err)
			if !errors.Is(uerr, context.Canceled) && !errors.Is(uerr, context.DeadlineExceeded) {
				// The client did not intentionally end the stream so this was an error in the
				// vstream itself.
				vs.vsm.vstreamsEndedWithErrors.Add(labelValues, 1)
			}
			vs.vsm.vstreamsCount.Add(labelValues, -1)
			vs.once.Do(func() {
				vs.setError(err, fmt.Sprintf("error starting stream from shard GTID %+v", sgtid))
				vs.cancel()
			})
		}
	}()
}

// MaxSkew is the threshold for a skew to be detected. Since MySQL timestamps are in seconds we account for
// two round-offs: one for the actual event and another while accounting for the clock skew
const MaxSkew = int64(2)

// computeSkew sets the timestamp of the current event for the calling stream, accounts for a clock skew
// and declares that a skew has arisen if the streams are too far apart
func (vs *vstream) computeSkew(streamID string, event *binlogdatapb.VEvent) bool {
	vs.skewMu.Lock()
	defer vs.skewMu.Unlock()
	// account for skew between this vtgate and the source mysql server
	secondsInThePast := event.CurrentTime/1e9 - event.Timestamp
	vs.timestamps[streamID] = time.Now().Unix() - secondsInThePast

	var minTs, maxTs int64
	var laggardStream string

	if len(vs.timestamps) <= 1 {
		return false
	}
	for k, ts := range vs.timestamps {
		if ts < minTs || minTs == 0 {
			minTs = ts
			laggardStream = k
		}
		if ts > maxTs {
			maxTs = ts
		}
	}
	if vs.laggard != "" { // we are skewed, check if this event has fixed the skew
		if (maxTs - minTs) <= MaxSkew {
			vs.laggard = ""
			close(vs.skewCh)
		}
	} else {
		if (maxTs - minTs) > MaxSkew { // check if we are skewed due to this event
			log.Infof("Skew found, laggard is %s, %+v", laggardStream, vs.timestamps)
			vs.laggard = laggardStream
			vs.skewCh = make(chan bool)
		}
	}
	return vs.mustPause(streamID)
}

// mustPause returns true if a skew exists and the stream calling this is not the slowest one
func (vs *vstream) mustPause(streamID string) bool {
	switch vs.laggard {
	case "":
		return false
	case streamID:
		// current stream is the laggard, not pausing
		return false
	}

	if (vs.timestamps[streamID] - vs.lowestTS) <= MaxSkew {
		// current stream is not the laggard, but the skew is still within the limit
		return false
	}
	vs.vsm.RecordStreamDelay()
	return true
}

// alignStreams is called by each individual shard's stream before an event is sent to the client or after each heartbeat.
// It checks for skew (if the minimizeSkew option is set). If skew is present this stream is delayed until the skew is fixed
// The faster stream detects the skew and waits. The slower stream resets the skew when it catches up.
func (vs *vstream) alignStreams(ctx context.Context, event *binlogdatapb.VEvent, keyspace, shard string) error {
	if !vs.minimizeSkew || event.Timestamp == 0 {
		return nil
	}
	streamID := fmt.Sprintf("%s/%s", keyspace, shard)
	for {
		mustPause := vs.computeSkew(streamID, event)
		if event.Type == binlogdatapb.VEventType_HEARTBEAT {
			return nil
		}
		if !mustPause {
			return nil
		}
		select {
		case <-ctx.Done():
			return vterrors.Wrapf(ctx.Err(), "context ended while waiting for skew to reduce for stream %s from %s/%s",
				streamID, keyspace, shard)
		case <-time.After(time.Duration(vs.skewTimeoutSeconds) * time.Second):
			log.Errorf("timed out while waiting for skew to reduce: %s", streamID)
			return vterrors.Errorf(vtrpcpb.Code_CANCELED, "timed out while waiting for skew to reduce for stream %s from %s/%s",
				streamID, keyspace, shard)
		case <-vs.skewCh:
			// once skew is fixed the channel is closed and all waiting streams "wake up"
		}
	}
}

func (vs *vstream) getCells() []string {
	var cells []string
	if vs.optCells != "" {
		for _, cell := range strings.Split(strings.TrimSpace(vs.optCells), ",") {
			cells = append(cells, strings.TrimSpace(cell))
		}
	}

	if len(cells) == 0 {
		// use the vtgate's cell by default
		cells = append(cells, vs.vsm.cell)
	}
	return cells
}

// streamFromTablet streams from one shard. If transactions come in separate chunks, they are grouped and sent.
func (vs *vstream) streamFromTablet(ctx context.Context, sgtid *binlogdatapb.ShardGtid) error {
	// journalDone is assigned a channel when a journal event is encountered.
	// It will be closed when all journal events converge.
	var journalDone chan struct{}
	ignoreTablets := make([]*topodatapb.TabletAlias, 0)
	labelValues := []string{sgtid.Keyspace, sgtid.Shard, vs.tabletType.String()}

	errCount := 0
	for {
		select {
		case <-ctx.Done():
			return vterrors.Wrapf(ctx.Err(), "context ended while streaming from %s/%s", sgtid.Keyspace, sgtid.Shard)
		case <-journalDone:
			// Unreachable.
			// This can happen if a server misbehaves and does not end
			// the stream after we return an error.
			return nil
		default:
		}

		var eventss [][]*binlogdatapb.VEvent
		var err error
		cells := vs.getCells()

		tpo := vs.tabletPickerOptions
		resharded, err := vs.keyspaceHasBeenResharded(ctx, sgtid.Keyspace)
		if err != nil {
			return vterrors.Wrapf(err, "failed to determine if keyspace %s has been resharded", sgtid.Keyspace)
		}
		if resharded {
			// The non-serving tablet in the old / non-serving shard will contain all of
			// the GTIDs that we need before transitioning to the new shards along with
			// the journal event that will then allow us to automatically transition to
			// the new shards (provided the stop_on_reshard option is not set).
			tpo.IncludeNonServingTablets = true
		}

		tabletPickerErr := func(err error) error {
			tperr := vterrors.Wrapf(err, "failed to find a %s tablet for VStream in %s/%s within the %s cell(s)",
				vs.tabletType.String(), sgtid.GetKeyspace(), sgtid.GetShard(), strings.Join(cells, ","))
			log.Errorf("%v", tperr)
			return tperr
		}
		tp, err := discovery.NewTabletPicker(ctx, vs.ts, cells, vs.vsm.cell, sgtid.GetKeyspace(), sgtid.GetShard(), vs.tabletType.String(), tpo, ignoreTablets...)
		if err != nil {
			return tabletPickerErr(err)
		}
		// Create a child context with a stricter timeout when picking a tablet.
		// This will prevent hanging in the case no tablets are found.
		tpCtx, tpCancel := context.WithTimeout(ctx, tabletPickerContextTimeout)
		defer tpCancel()
		tablet, err := tp.PickForStreaming(tpCtx)
		if err != nil {
			return tabletPickerErr(err)
		}
		tabletAliasString := topoproto.TabletAliasString(tablet.Alias)
		log.Infof("Picked %s tablet %s for VStream in %s/%s within the %s cell(s)",
			vs.tabletType.String(), tabletAliasString, sgtid.GetKeyspace(), sgtid.GetShard(), strings.Join(cells, ","))

		target := &querypb.Target{
			Keyspace:   sgtid.Keyspace,
			Shard:      sgtid.Shard,
			TabletType: vs.tabletType,
			Cell:       vs.vsm.cell,
		}
		tabletConn, err := vs.vsm.resolver.GetGateway().QueryServiceByAlias(ctx, tablet.Alias, target)
		if err != nil {
			log.Errorf(err.Error())
			return vterrors.Wrapf(err, "failed to get tablet connection to %s", tabletAliasString)
		}

		errCh := make(chan error, 1)
		go func() {
			_ = tabletConn.StreamHealth(ctx, func(shr *querypb.StreamHealthResponse) error {
				var err error
				switch {
				case ctx.Err() != nil:
					err = vterrors.Wrapf(ctx.Err(), "context ended while streaming tablet health from %s", tabletAliasString)
				case shr == nil || shr.RealtimeStats == nil || shr.Target == nil:
					err = fmt.Errorf("health check failed on %s", tabletAliasString)
				case vs.tabletType != shr.Target.TabletType:
					err = fmt.Errorf("tablet %s type has changed from %s to %s, restarting vstream",
						topoproto.TabletAliasString(tablet.Alias), vs.tabletType, shr.Target.TabletType)
				case shr.RealtimeStats.HealthError != "":
					err = fmt.Errorf("tablet %s is no longer healthy: %s, restarting vstream",
						topoproto.TabletAliasString(tablet.Alias), shr.RealtimeStats.HealthError)
				case shr.RealtimeStats.ReplicationLagSeconds > uint32(discovery.GetLowReplicationLag().Seconds()):
					err = fmt.Errorf("tablet %s has a replication lag of %d seconds which is beyond the value provided in --discovery_low_replication_lag of %s so the tablet is no longer considered healthy, restarting vstream",
						topoproto.TabletAliasString(tablet.Alias), shr.RealtimeStats.ReplicationLagSeconds, discovery.GetLowReplicationLag())
				}
				if err != nil {
					log.Warningf("Tablet state changed: %s, attempting to restart", err)
					err = vterrors.Wrapf(err, "error streaming tablet health from %s", tabletAliasString)
					errCh <- err
					return err
				}
				return nil
			})
		}()

		var options *binlogdatapb.VStreamOptions
		const SidecarDBHeartbeatTableName = "heartbeat"
		if vs.flags.GetStreamKeyspaceHeartbeats() {
			options = &binlogdatapb.VStreamOptions{
				InternalTables: []string{SidecarDBHeartbeatTableName},
			}
		}

		if options != nil {
			options.TablesToCopy = vs.flags.GetTablesToCopy()
		} else {
			options = &binlogdatapb.VStreamOptions{
				TablesToCopy: vs.flags.GetTablesToCopy(),
			}
		}

		// Safe to access sgtid.Gtid here (because it can't change until streaming begins).
		req := &binlogdatapb.VStreamRequest{
			Target:       target,
			Position:     sgtid.Gtid,
			Filter:       vs.filter,
			TableLastPKs: sgtid.TablePKs,
			Options:      options,
		}
		log.Infof("Starting to vstream from %s, with req %+v", tabletAliasString, req)
		err = tabletConn.VStream(ctx, req, func(events []*binlogdatapb.VEvent) error {
			// We received a valid event. Reset error count.
			errCount = 0

			select {
			case <-ctx.Done():
				return vterrors.Wrapf(ctx.Err(), "context ended while streaming from tablet %s in %s/%s",
					tabletAliasString, sgtid.Keyspace, sgtid.Shard)
			case streamErr := <-errCh:
				log.Infof("vstream for %s/%s ended due to health check, should retry: %v", sgtid.Keyspace, sgtid.Shard, streamErr)
				// You must return Code_UNAVAILABLE here to trigger a restart.
				return vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "error streaming from tablet %s in %s/%s: %s",
					tabletAliasString, sgtid.Keyspace, sgtid.Shard, streamErr.Error())
			case <-journalDone:
				// Unreachable.
				// This can happen if a server misbehaves and does not end
				// the stream after we return an error.
				log.Infof("vstream for %s/%s ended due to journal event, returning io.EOF", sgtid.Keyspace, sgtid.Shard)
				return io.EOF
			default:
			}

			aligningStreamsErr := fmt.Sprintf("error aligning streams across %s/%s", sgtid.Keyspace, sgtid.Shard)
			sendingEventsErr := fmt.Sprintf("error sending event batch from tablet %s", tabletAliasString)

			sendevents := make([]*binlogdatapb.VEvent, 0, len(events))
			for i, event := range events {
				switch event.Type {
				case binlogdatapb.VEventType_FIELD:
					ev := maybeUpdateTableName(event, sgtid.Keyspace, vs.flags.GetExcludeKeyspaceFromTableName(), extractFieldTableName)
					sendevents = append(sendevents, ev)
				case binlogdatapb.VEventType_ROW:
					ev := maybeUpdateTableName(event, sgtid.Keyspace, vs.flags.GetExcludeKeyspaceFromTableName(), extractRowTableName)
					sendevents = append(sendevents, ev)
				case binlogdatapb.VEventType_COMMIT, binlogdatapb.VEventType_DDL, binlogdatapb.VEventType_OTHER:
					sendevents = append(sendevents, event)
					eventss = append(eventss, sendevents)

					if err := vs.alignStreams(ctx, event, sgtid.Keyspace, sgtid.Shard); err != nil {
						return vterrors.Wrap(err, aligningStreamsErr)
					}

					if err := vs.sendAll(ctx, sgtid, eventss); err != nil {
						log.Infof("vstream for %s/%s, error in sendAll: %v", sgtid.Keyspace, sgtid.Shard, err)
						return vterrors.Wrap(err, sendingEventsErr)
					}
					eventss = nil
					sendevents = nil
				case binlogdatapb.VEventType_COPY_COMPLETED:
					sendevents = append(sendevents, event)
					if fullyCopied, doneEvent := vs.isCopyFullyCompleted(ctx, sgtid, event); fullyCopied {
						sendevents = append(sendevents, doneEvent)
					}
					eventss = append(eventss, sendevents)

					if err := vs.alignStreams(ctx, event, sgtid.Keyspace, sgtid.Shard); err != nil {
						return vterrors.Wrap(err, aligningStreamsErr)
					}

					if err := vs.sendAll(ctx, sgtid, eventss); err != nil {
						log.Infof("vstream for %s/%s, error in sendAll, on copy completed event: %v", sgtid.Keyspace, sgtid.Shard, err)
						return vterrors.Wrap(err, sendingEventsErr)
					}
					eventss = nil
					sendevents = nil
				case binlogdatapb.VEventType_HEARTBEAT:
					// Remove all heartbeat events for now.
					// Otherwise they can accumulate indefinitely if there are no real events.
					// TODO(sougou): figure out a model for this.
					if err := vs.alignStreams(ctx, event, sgtid.Keyspace, sgtid.Shard); err != nil {
						return vterrors.Wrap(err, aligningStreamsErr)
					}
				case binlogdatapb.VEventType_JOURNAL:
					journal := event.Journal
					// Journal events are not sent to clients by default, but only when
					// IncludeReshardJournalEvents or StopOnReshard is set.
					if (vs.includeReshardJournalEvents || vs.stopOnReshard) &&
						journal.MigrationType == binlogdatapb.MigrationType_SHARDS {
						sendevents = append(sendevents, event)
						// Read any subsequent events until we get the VGTID->COMMIT events that
						// always follow the JOURNAL event which is generated as a result of
						// an autocommit insert into the _vt.resharding_journal table on the
						// tablet.
						for j := i + 1; j < len(events); j++ {
							sendevents = append(sendevents, events[j])
							if events[j].Type == binlogdatapb.VEventType_COMMIT {
								break
							}
						}
						eventss = append(eventss, sendevents)
						if err := vs.sendAll(ctx, sgtid, eventss); err != nil {
							log.Infof("vstream for %s/%s, error in sendAll, on journal event: %v", sgtid.Keyspace, sgtid.Shard, err)
							return vterrors.Wrap(err, sendingEventsErr)
						}
						eventss = nil
						sendevents = nil
					}
					je, err := vs.getJournalEvent(ctx, sgtid, journal)
					if err != nil {
						return vterrors.Wrapf(err, "error getting journal event for shard GTID %+v on tablet %s",
							sgtid, tabletAliasString)
					}
					if je != nil {
						var endTimer *time.Timer
						if vs.stopOnReshard {
							// We're going to be ending the tablet stream, along with the VStream, so
							// we ensure a reasonable minimum amount of time is alloted for clients
							// to Recv the journal event before the VStream's context is cancelled
							// (which would cause the grpc SendMsg or RecvMsg to fail). If the client
							// doesn't Recv the journal event before the VStream ends then they'll
							// have to resume from the last ShardGtid they received before the
							// journal event.
							endTimer = time.NewTimer(stopOnReshardDelay)
							defer endTimer.Stop()
						}
						// Wait until all other participants converge and then return EOF after
						// any minimum delay has passed.
						journalDone = je.done
						select {
						case <-ctx.Done():
							return vterrors.Wrapf(ctx.Err(), "context ended while waiting for journal event for shard GTID %+v on tablet %s",
								sgtid, tabletAliasString)
						case <-journalDone:
							if endTimer != nil {
								<-endTimer.C
							}
							log.Infof("vstream for %s/%s ended due to journal event, returning io.EOF", sgtid.Keyspace, sgtid.Shard)
							return io.EOF
						}
					}
				default:
					sendevents = append(sendevents, event)
				}
				lag := event.CurrentTime/1e9 - event.Timestamp
				vs.vsm.vstreamsLag.Set(labelValues, lag)
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
			err = vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "vstream ended unexpectedly on tablet %s in %s/%s",
				tabletAliasString, sgtid.Keyspace, sgtid.Shard)
		}

		retry, ignoreTablet := vs.shouldRetry(err)
		if !retry {
			log.Infof("vstream for %s/%s error, no retry: %v", sgtid.Keyspace, sgtid.Shard, err)
			return vterrors.Wrapf(err, "error in vstream for %s/%s on tablet %s",
				sgtid.Keyspace, sgtid.Shard, tabletAliasString)
		}
		if ignoreTablet {
			ignoreTablets = append(ignoreTablets, tablet.GetAlias())
		}

		errCount++
		// Retry, at most, 3 times if the error can be retried.
		if errCount >= 3 {
			log.Errorf("vstream for %s/%s had three consecutive failures: %v", sgtid.Keyspace, sgtid.Shard, err)
			return vterrors.Wrapf(err, "persistent error in vstream for %s/%s on tablet %s; giving up",
				sgtid.Keyspace, sgtid.Shard, tabletAliasString)
		}
		log.Infof("vstream for %s/%s error, retrying: %v", sgtid.Keyspace, sgtid.Shard, err)
	}

}

// maybeUpdateTableNames updates table names when the ExcludeKeyspaceFromTableName flag is disabled.
// If we're streaming from multiple keyspaces, updating the table names by inserting the keyspace will disambiguate
// duplicate table names. If we enable the ExcludeKeyspaceFromTableName flag to not update the table names, there is no need to
// clone the entire event, whcih improves performance. This is typically safely used by clients only streaming one keyspace.
func maybeUpdateTableName(event *binlogdatapb.VEvent, keyspace string, excludeKeyspaceFromTableName bool,
	tableNameExtractor func(ev *binlogdatapb.VEvent) *string) *binlogdatapb.VEvent {
	if excludeKeyspaceFromTableName {
		return event
	}
	ev := event.CloneVT()
	tableName := tableNameExtractor(ev)
	*tableName = keyspace + "." + *tableName
	return ev
}

func extractFieldTableName(ev *binlogdatapb.VEvent) *string {
	return &ev.FieldEvent.TableName
}

func extractRowTableName(ev *binlogdatapb.VEvent) *string {
	return &ev.RowEvent.TableName
}

// shouldRetry determines whether we should exit immediately or retry the vstream.
// The first return value determines if the error can be retried, while the second
// indicates whether the tablet with which the error occurred should be omitted
// from the candidate list of tablets to choose from on the retry.
//
// An error should be retried if it is expected to be transient.
// A tablet should be ignored upon retry if it's likely another tablet will not
// produce the same error.
func (vs *vstream) shouldRetry(err error) (retry bool, ignoreTablet bool) {
	errCode := vterrors.Code(err)
	// In this context, where we will run the tablet picker again on retry, these
	// codes indicate that it's worth a retry as the error is likely a transient
	// one with a tablet or within the shard.
	if errCode == vtrpcpb.Code_FAILED_PRECONDITION || errCode == vtrpcpb.Code_UNAVAILABLE {
		return true, false
	}
	// This typically indicates that the user provided invalid arguments for the
	// VStream so we should not retry.
	if errCode == vtrpcpb.Code_INVALID_ARGUMENT {
		// But if there is a GTIDSet Mismatch on the tablet, omit that tablet from
		// the candidate list in the TabletPicker and retry. The argument was invalid
		// *for that specific *tablet* but it's not generally invalid.
		if strings.Contains(err.Error(), "GTIDSet Mismatch") {
			return true, true
		}
		return false, false
	}
	// Internal errors such as not having all journaling partipants require a new
	// VStream.
	if errCode == vtrpcpb.Code_INTERNAL {
		return false, false
	}

	// For anything else, if this is an ephemeral SQL error -- such as a
	// MAX_EXECUTION_TIME SQL error during the copy phase -- or any other
	// type of non-SQL error, then retry.
	return sqlerror.IsEphemeralError(err), false
}

// sendAll sends a group of events together while holding the lock.
func (vs *vstream) sendAll(ctx context.Context, sgtid *binlogdatapb.ShardGtid, eventss [][]*binlogdatapb.VEvent) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	labelValues := []string{sgtid.Keyspace, sgtid.Shard, vs.tabletType.String()}

	// Send all chunks while holding the lock.
	for _, events := range eventss {
		if err := vs.getError(); err != nil {
			return err
		}
		// Convert all gtids to vgtids. This should be done here while holding the lock.
		for j, event := range events {
			if event.Type == binlogdatapb.VEventType_GTID {
				// Update the VGtid and send that instead.
				sgtid.Gtid = event.Gtid
				events[j] = &binlogdatapb.VEvent{
					Type:     binlogdatapb.VEventType_VGTID,
					Vgtid:    vs.vgtid.CloneVT(),
					Keyspace: event.Keyspace,
					Shard:    event.Shard,
				}
			} else if event.Type == binlogdatapb.VEventType_LASTPK {
				var foundIndex = -1
				eventTablePK := event.LastPKEvent.TableLastPK
				for idx, pk := range sgtid.TablePKs {
					if pk.TableName == eventTablePK.TableName {
						foundIndex = idx
						break
					}
				}
				if foundIndex == -1 {
					if !event.LastPKEvent.Completed {
						sgtid.TablePKs = append(sgtid.TablePKs, eventTablePK)
					}
				} else {
					if event.LastPKEvent.Completed {
						// remove tablepk from sgtid
						sgtid.TablePKs[foundIndex] = sgtid.TablePKs[len(sgtid.TablePKs)-1]
						sgtid.TablePKs[len(sgtid.TablePKs)-1] = nil
						sgtid.TablePKs = sgtid.TablePKs[:len(sgtid.TablePKs)-1]
					} else {
						sgtid.TablePKs[foundIndex] = eventTablePK
					}
				}
				events[j] = &binlogdatapb.VEvent{
					Type:     binlogdatapb.VEventType_VGTID,
					Vgtid:    vs.vgtid.CloneVT(),
					Keyspace: event.Keyspace,
					Shard:    event.Shard,
				}
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case vs.eventCh <- events:
			vs.vsm.vstreamsEventsStreamed.Add(labelValues, int64(len(events)))
		}
	}
	return nil
}

// isCopyFullyCompleted returns true if all stream has received a copy_completed event.
// If true, it will also return a new copy_completed event that needs to be sent.
// This new event represents the completion of all the copy operations.
func (vs *vstream) isCopyFullyCompleted(ctx context.Context, sgtid *binlogdatapb.ShardGtid, event *binlogdatapb.VEvent) (bool, *binlogdatapb.VEvent) {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	vs.copyCompletedShard[fmt.Sprintf("%s/%s", event.Keyspace, event.Shard)] = struct{}{}

	for _, shard := range vs.vgtid.ShardGtids {
		if _, ok := vs.copyCompletedShard[fmt.Sprintf("%s/%s", shard.Keyspace, shard.Shard)]; !ok {
			return false, nil
		}
	}
	return true, &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_COPY_COMPLETED,
	}
}

func (vs *vstream) getError() error {
	vs.errMu.Lock()
	defer vs.errMu.Unlock()
	return vs.err
}

func (vs *vstream) setError(err error, msg string) {
	vs.errMu.Lock()
	defer vs.errMu.Unlock()
	vs.err = vterrors.Wrap(err, msg)
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
		log.Infof("Journal event received: %v", journal)
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
						return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "not all journaling participants are in the stream: journal: %v, stream: %v",
							journal.Participants, vs.vgtid.ShardGtids)
					}
					continue nextParticipant
				}
			}
			switch mode {
			case undecided, matchNone:
				mode = matchNone
			case matchAll:
				return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "not all journaling participants are in the stream: journal: %v, stream: %v",
					journal.Participants, vs.vgtid.ShardGtids)
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

	if !vs.stopOnReshard { // stop streaming from current shards and start streaming the new shards
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
			// It's ok to start the streams even though ShardGtids are not updated yet.
			// This is because we're still holding the lock.
			vs.startOneStream(ctx, sgtid)
		}
		vs.vgtid.ShardGtids = newsgtids
	}
	close(je.done)
	return je, nil
}

// keyspaceHasBeenResharded returns true if the keyspace's serving shard set has changed
// since the last VStream as indicated by the shard definitions provided in the VGTID.
func (vs *vstream) keyspaceHasBeenResharded(ctx context.Context, keyspace string) (bool, error) {
	shards, err := vs.ts.FindAllShardsInKeyspace(ctx, keyspace, nil)
	if err != nil || len(shards) == 0 {
		return false, err
	}

	vs.mu.Lock()
	defer vs.mu.Unlock()

	// First check the typical case, where the VGTID shards match the serving shards.
	// In that case it's NOT possible that an applicable reshard has happened because
	// the VGTID contains shards that are all serving.
	reshardPossible := false
	ksShardGTIDs := make([]*binlogdatapb.ShardGtid, 0, len(vs.vgtid.ShardGtids))
	for _, s := range vs.vgtid.ShardGtids {
		if s.GetKeyspace() == keyspace {
			ksShardGTIDs = append(ksShardGTIDs, s)
		}
	}
	for _, s := range ksShardGTIDs {
		shard := shards[s.GetShard()]
		if shard == nil {
			return false, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "shard provided in VGTID, %s, not found in the %s keyspace",
				s.GetShard(), keyspace)
		}
		if !shard.GetIsPrimaryServing() {
			reshardPossible = true
			break
		}
	}
	if !reshardPossible {
		return false, nil
	}

	// Now that we know there MAY have been an applicable reshard, let's make a
	// definitive determination by looking at the shard keyranges.
	// All we care about are the shard info records now.
	sis := maps.Values(shards)
	for i := range sis {
		for j := range sis {
			if sis[i].ShardName() == sis[j].ShardName() && key.KeyRangeEqual(sis[i].GetKeyRange(), sis[j].GetKeyRange()) {
				// It's the same shard so skip it.
				continue
			}
			if key.KeyRangeIntersect(sis[i].GetKeyRange(), sis[j].GetKeyRange()) {
				// We have different shards with overlapping keyranges so we know
				// that a reshard has happened.
				return true, nil
			}
		}
	}

	return false, nil
}
