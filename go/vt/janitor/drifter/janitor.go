package drifter

import (
	"sync"
	"time"

	"github.com/youtube/vitess/go/history"
	"github.com/youtube/vitess/go/vt/janitor"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"

	_ "github.com/youtube/vitess/go/vt/tabletmanager/gorpctmclient"
)

const historyLength = 128

// TabletState replication state (understood as correct or drifting)
// of a tablet.
type TabletState struct {
	Tablet         *topo.TabletInfo
	ExpectedMaster topo.TabletAlias
}

// Good returns true if the tablet replication state is correct (has
// the right master). Good is true for a zero TabletState.
func (state TabletState) Good() bool {
	return state.Tablet == nil || state.ExpectedMaster == state.ActualMaster()
}

func (state TabletState) ActualMaster() topo.TabletAlias {
	return state.Tablet.Parent
}

// StateChange represents how the replication state of a tablet has
// changed after applying a new state.
type StateChange int

const (
	NotDrifting StateChange = iota
	StillDrifting
	StartedDrifting
	StoppedDrifting
)

// Apply applies a new tablet state to our current status.
func (janitor *Janitor) Apply(state TabletState) StateChange {
	if state.Good() == janitor.status[state.Tablet.Alias].Good() {
		if state.Good() {
			return NotDrifting
		}
		return StillDrifting
	}
	janitor.status[state.Tablet.Alias] = state

	if state.Good() {
		return StoppedDrifting
	}
	return StartedDrifting
}

// types of records:
// - we found a drifter: !IsFix && !Good()
// - a drifter is confirmed to be fixed: !IsFix && Good()
// - we called SlaveWasRestarted, did it return an error?

// A record is an entry in the history.
type Record struct {
	// TabletState is the tablet state whose handling we are
	// recording.
	TabletState
	// IsFix is true if this is recording of an attempt to fix a
	// tablet.
	IsFix bool
	// Timestamp of the event.
	Time time.Time
	// Error that happened while fixing a tablet (only set if
	// IsFix is true).
	Err error
}

// Janitor monitors tablets that have the wrong master and tries to
// fix them.
type Janitor struct {
	Keyspace string
	Shard    string

	ts       topo.Server
	wr       *wrangler.Wrangler
	fixQueue chan TabletState

	statusMu sync.RWMutex
	status   map[topo.TabletAlias]TabletState

	History *history.History
}

func (janitor *Janitor) Configure(wr *wrangler.Wrangler, keyspace, shard string) error {
	janitor.ts = wr.TopoServer()
	janitor.wr = wr
	janitor.Keyspace = keyspace
	janitor.Shard = shard
	return nil
}

// NewJanitor returns a janitor that will monitor keyspaces in cell.
func New() *Janitor {
	return &Janitor{
		History:  history.New(historyLength),
		fixQueue: make(chan TabletState),
		status:   make(map[topo.TabletAlias]TabletState),
	}
}

func (janitor *Janitor) BadTablets() (badTablets []*topo.TabletInfo) {
	janitor.statusMu.RLock()
	defer janitor.statusMu.RUnlock()

	for _, state := range janitor.status {
		if !state.Good() {
			badTablets = append(badTablets, state.Tablet)
		}
	}

	return badTablets
}

func init() {
	janitor.Register("drifter", New())
}
