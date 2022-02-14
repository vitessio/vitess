package events

import (
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// ExternalVitessClusterChange is an event that describes changes to a vitess cluster.
type ExternalVitessClusterChange struct {
	ClusterName           string
	ExternalVitessCluster *topodatapb.ExternalVitessCluster
	Status                string
}
