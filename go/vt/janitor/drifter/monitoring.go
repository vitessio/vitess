package drifter

import (
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
)

var (
	varDrifterCount       = stats.NewCounters("DriftingTabletsCount")
	varSlaveRestartCount  = stats.NewInt("SlaveRestartCount")
	varSlaveRestartErrors = stats.NewInt("SlaveRestartErrors")
)
var (
	actionTimeout = 60 * time.Second
)

// HandleEvent adds a state to the status of the janitor and
// dispatches a fix for it if necessary.
func (janitor *Janitor) HandleEvent(state TabletState) (drifting bool) {
	result := janitor.Apply(state)

	if result == StartedDrifting {
		janitor.History.Add(Record{
			TabletState: state,
			Time:        time.Now(),
		})
		varDrifterCount.Add(state.Tablet.Keyspace, 1)
	}

	switch result {
	case StillDrifting, StartedDrifting:
		log.Infof("%v follows the wrong master: want %v, got %v", state.Tablet.Alias, state.ExpectedMaster, state.Tablet.Parent)
		return true
	case StoppedDrifting:
		janitor.History.Add(Record{
			TabletState: state,
			Time:        time.Now(),
		})
		log.Infof("%v is fixed and now has the correct master: %v", state.Tablet.Alias, state.ExpectedMaster)
		varDrifterCount.Add(state.Tablet.Alias.Cell, -1)
	}
	return false
}

func (janitor *Janitor) checkTablets(tabletMap map[topo.TabletAlias]*topo.TabletInfo) (drifting []TabletState, err error) {
	shardInfo, err := janitor.ts.GetShardCritical(janitor.Keyspace, janitor.Shard)
	if err != nil {
		return nil, err
	}
	janitor.statusMu.Lock()
	defer janitor.statusMu.Unlock()

	for _, tablet := range tabletMap {

		if tablet.Type == topo.TYPE_MASTER {
			continue
		}
		state := TabletState{
			Tablet:         tablet,
			ExpectedMaster: shardInfo.MasterAlias,
		}
		log.Infof("looking at tablet %+v, state: %v", state, state.Good())
		if janitor.HandleEvent(state) {
			drifting = append(drifting, state)
		}
	}
	return drifting, nil
}

// Run looks tries to find and fix any tablets that are drifting in
// the keyspace and shard, in all cells.
func (janitor *Janitor) Run(active bool) error {
	cells, err := janitor.ts.GetKnownCells()
	if err != nil {
		return err
	}

	tabletMap, err := wrangler.GetTabletMapForShardByCell(janitor.ts, janitor.Keyspace, janitor.Shard, cells)
	if err != nil && err != topo.ErrPartialResult {
		return err
	}
	drifters, err := janitor.checkTablets(tabletMap)
	if err != nil {
		return err
	}

	if !active {
		for _, state := range drifters {
			log.Infof("dry run mode: not running slaveWasRestarted for %v", state.Tablet)
		}
		return nil
	}

	// Try to fix the tablets.
	for _, state := range drifters {
		if err := janitor.slaveWasRestarted(state); err != nil {
			return err
		}
	}

	// Recheck the drifters: hopefully we managed to fix them.

	var driftingAliases []topo.TabletAlias
	for _, state := range drifters {
		driftingAliases = append(driftingAliases, state.Tablet.Alias)
	}
	driftingTabletMap, err := wrangler.GetTabletMap(janitor.ts, driftingAliases)
	if err != nil {
		return err
	}
	_, err = janitor.checkTablets(driftingTabletMap)
	return err

}

func (janitor *Janitor) slaveWasRestarted(state TabletState) error {
	record := Record{
		Time:        time.Now(),
		TabletState: state,
		IsFix:       true,
	}
	defer janitor.History.Add(record)

	janitor.wr.ResetActionTimeout(actionTimeout)
	log.Infof("calling SlaveWasRestarted(%v, %v, false)", state.Tablet.Alias, state.ExpectedMaster)

	defer varSlaveRestartCount.Add(1)
	if err := janitor.wr.SlaveWasRestarted(state.Tablet.Alias, state.ExpectedMaster, false); err != nil {
		record.Err = err
		varSlaveRestartErrors.Add(1)
		return err
	}
	return nil
}
