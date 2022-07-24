package vttest

import (
	"context"
	"time"

	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vttest"
	"vitess.io/vitess/go/vt/topo"
)

// TopoManager is an interface to manage things in a remote topology server for local cluster
type TopoManager interface {
	Setup() error
}

// Topoctl implements TopoManager. For now it only sets up cells in remote topology server if any of them doesn't exist.
type Topoctl struct {
	TopoImplementation      string
	TopoGlobalServerAddress string
	TopoGlobalRoot          string
	Topology                *vttest.VTTestTopology
}

func (ctl *Topoctl) Setup() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	topoServer, err := topo.OpenServer(ctl.TopoImplementation, ctl.TopoGlobalServerAddress, ctl.TopoGlobalRoot)
	if err != nil {
		return err
	}

	log.Infof("Creating cells if they don't exist in the provided topo server %s %s %s", ctl.TopoImplementation, ctl.TopoGlobalServerAddress, ctl.TopoGlobalRoot)
	// Create cells if it doesn't exist to be idempotent. Should work when we share the same topo server across multiple local clusters.
	for _, cell := range ctl.Topology.Cells {
		_, err := topoServer.GetCellInfo(ctx, cell, true)
		// Cell info already exists. no-op
		if err == nil {
			continue
		}

		// Return any error that's not NoNode
		if !topo.IsErrType(err, topo.NoNode) {
			return err
		}

		// Use the same topo server address in cell info, or else it would cause error when talking to local cell topo
		// Use dummy (empty) cell root till we have a use case to set up local cells properly
		cellInfo := &topodatapb.CellInfo{ServerAddress: ctl.TopoGlobalServerAddress}

		err = topoServer.CreateCellInfo(ctx, cell, cellInfo)
		if err != nil {
			return err
		}
		log.Infof("Created cell info for %s in the topo server %s %s %s", cell, ctl.TopoImplementation, ctl.TopoGlobalServerAddress, ctl.TopoGlobalRoot)
	}

	return nil
}
