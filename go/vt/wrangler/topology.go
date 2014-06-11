package wrangler

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
)

// TabletNode is the representation of a tablet in the db topology.
// It can be constructed from a Tablet object, or from an EndPoint.
type TabletNode struct {
	Host  string
	Alias topo.TabletAlias
	Port  int
}

// ShortName returns a displayable representation of the host name.
func (tn *TabletNode) ShortName() string {
	hostPart := strings.SplitN(tn.Host, ".", 2)[0]
	if tn.Port == 0 {
		return hostPart
	}
	return fmt.Sprintf("%v:%v", hostPart, tn.Port)
}

func newTabletNodeFromTabletInfo(ti *topo.TabletInfo) *TabletNode {
	if err := ti.ValidatePortmap(); err != nil {
		log.Errorf("ValidatePortmap(%v): %v", ti.Alias, err)
	}
	return &TabletNode{
		Host:  ti.Hostname,
		Port:  ti.Portmap["vt"],
		Alias: ti.Alias,
	}
}

func newTabletNodeFromEndPoint(ep topo.EndPoint, cell string) *TabletNode {
	return &TabletNode{
		Host: ep.Host,
		Alias: topo.TabletAlias{
			Uid:  ep.Uid,
			Cell: cell},
		Port: ep.NamedPortMap[topo.DefaultPortName],
	}
}

// TabletNodesByType maps tablet types to slices of tablet nodes.
type TabletNodesByType map[topo.TabletType][]*TabletNode

// ShardNodes represents all tablet nodes for a shard, indexed by tablet type.
type ShardNodes struct {
	Name        string
	TabletNodes TabletNodesByType
}

type numericShardNodesList []*ShardNodes

// Len is part of sort.Interface
func (nsnl numericShardNodesList) Len() int {
	return len(nsnl)
}

// Less is part of sort.Interface
func (nsnl numericShardNodesList) Less(i, j int) bool {
	// This panics, so it shouldn't be called unless all shard
	// names can be converted to integers.
	ii, err := strconv.Atoi(nsnl[i].Name)
	if err != nil {
		panic("bad numeric shard: " + nsnl[i].Name)
	}

	jj, err := strconv.Atoi(nsnl[j].Name)
	if err != nil {
		panic("bad numeric shard" + nsnl[j].Name)
	}
	return ii < jj

}

// Swap is part of sort.Interface
func (nsnl numericShardNodesList) Swap(i, j int) {
	nsnl[i], nsnl[j] = nsnl[j], nsnl[i]
}

type rangeShardNodesList []*ShardNodes

// Len is part of sort.Interface
func (rsnl rangeShardNodesList) Len() int {
	return len(rsnl)
}

// Less is part of sort.Interface
func (rsnl rangeShardNodesList) Less(i, j int) bool {
	return rsnl[i].Name < rsnl[j].Name
}

// Swap is part of sort.Interface
func (rsnl rangeShardNodesList) Swap(i, j int) {
	rsnl[i], rsnl[j] = rsnl[j], rsnl[i]
}

// KeyspaceNodes represents all tablet nodes in a keyspace.
type KeyspaceNodes struct {
	ShardNodes []*ShardNodes // sorted by shard name
}

func newKeyspaceNodes() *KeyspaceNodes {
	return &KeyspaceNodes{
		ShardNodes: make([]*ShardNodes, 0),
	}
}

func (ks *KeyspaceNodes) hasOnlyNumericShardNames() bool {
	for _, shardNodes := range ks.ShardNodes {
		if _, err := strconv.Atoi(shardNodes.Name); err != nil {
			return false
		}
	}
	return true
}

// TabletTypes returns a slice of tablet type names this ks
// contains.
func (ks KeyspaceNodes) TabletTypes() []topo.TabletType {
	contained := make([]topo.TabletType, 0)
	for _, t := range topo.AllTabletTypes {
		if ks.HasType(t) {
			contained = append(contained, t)
		}
	}
	return contained
}

// HasType returns true if ks has any tablets with the named type.
func (ks KeyspaceNodes) HasType(tabletType topo.TabletType) bool {
	for _, shardNodes := range ks.ShardNodes {
		if _, ok := shardNodes.TabletNodes[tabletType]; ok {
			return true
		}
	}
	return false
}

// Topology is the entire set of tablets in the topology.
type Topology struct {
	Assigned map[string]*KeyspaceNodes // indexed by keyspace name
	Idle     []*TabletNode
	Scrap    []*TabletNode
	Partial  bool
}

// DbTopology returns the Topology for the current topo server.
func (wr *Wrangler) DbTopology() (*Topology, error) {
	topology := &Topology{
		Assigned: make(map[string]*KeyspaceNodes),
		Idle:     make([]*TabletNode, 0),
		Scrap:    make([]*TabletNode, 0),
		Partial:  false,
	}

	tabletInfos, err := GetAllTabletsAccrossCells(wr.ts)
	switch err {
	case nil:
		// we're good, no error
	case topo.ErrPartialResult:
		// we got a partial result
		topology.Partial = true
	default:
		// we got no result at all
		return nil, err
	}

	assigned := make(map[string]map[string]TabletNodesByType)
	for _, ti := range tabletInfos {
		tablet := newTabletNodeFromTabletInfo(ti)
		switch ti.Type {
		case topo.TYPE_IDLE:
			topology.Idle = append(topology.Idle, tablet)
		case topo.TYPE_SCRAP:
			topology.Scrap = append(topology.Scrap, tablet)
		default:
			if _, ok := assigned[ti.Keyspace]; !ok {
				assigned[ti.Keyspace] = make(map[string]TabletNodesByType)
			}
			if _, ok := assigned[ti.Keyspace][ti.Shard]; !ok {
				assigned[ti.Keyspace][ti.Shard] = make(TabletNodesByType)
			}
			assigned[ti.Keyspace][ti.Shard][ti.Type] = append(assigned[ti.Keyspace][ti.Shard][ti.Type], tablet)
		}
	}

	for keyspace, shardMap := range assigned {
		kn := newKeyspaceNodes()
		for shard, nodes := range shardMap {
			kn.ShardNodes = append(kn.ShardNodes, &ShardNodes{
				Name:        shard,
				TabletNodes: nodes,
			})
		}
		if kn.hasOnlyNumericShardNames() {
			sort.Sort(numericShardNodesList(kn.ShardNodes))
		} else {
			sort.Sort(rangeShardNodesList(kn.ShardNodes))
		}
		topology.Assigned[keyspace] = kn
	}
	return topology, nil
}

// ServingGraph contains the representation of the serving graph
// for a given cell.
type ServingGraph struct {
	Cell      string
	Keyspaces map[string]*KeyspaceNodes // indexed by keyspace name
	Errors    []string                  // collected during creation
}

// ServingGraph returns the ServingGraph for the given cell.
func (wr *Wrangler) ServingGraph(cell string) (servingGraph *ServingGraph) {
	servingGraph = &ServingGraph{
		Cell:      cell,
		Keyspaces: make(map[string]*KeyspaceNodes),
	}

	keyspaces, err := wr.ts.GetSrvKeyspaceNames(cell)
	if err != nil {
		servingGraph.Errors = append(servingGraph.Errors, fmt.Sprintf("GetSrvKeyspaceNames returned: %v", err))
		return
	}
	for _, keyspace := range keyspaces {
		servingGraph.Keyspaces[keyspace] = newKeyspaceNodes()
		shards, err := wr.ts.GetShardNames(keyspace)
		if err != nil {
			servingGraph.Errors = append(servingGraph.Errors, fmt.Sprintf("GetShardNames(%v) returned: %v", keyspace, err))
			continue
		}
		for _, shard := range shards {
			sn := &ShardNodes{
				Name:        shard,
				TabletNodes: make(TabletNodesByType),
			}
			servingGraph.Keyspaces[keyspace].ShardNodes = append(servingGraph.Keyspaces[keyspace].ShardNodes, sn)
			tabletTypes, err := wr.ts.GetSrvTabletTypesPerShard(cell, keyspace, shard)
			if err != nil {
				servingGraph.Errors = append(servingGraph.Errors, fmt.Sprintf("GetSrvTabletTypesPerShard(%v, %v, %v) returned: %v", cell, keyspace, shard, err))
				continue
			}
			for _, tabletType := range tabletTypes {
				endPoints, err := wr.ts.GetEndPoints(cell, keyspace, shard, tabletType)
				if err != nil {
					servingGraph.Errors = append(servingGraph.Errors, fmt.Sprintf("GetEndPoints(%v, %v, %v, %v) returned: %v", cell, keyspace, shard, tabletType, err))
					continue
				}
				for _, endPoint := range endPoints.Entries {
					sn.TabletNodes[tabletType] = append(sn.TabletNodes[tabletType], newTabletNodeFromEndPoint(endPoint, cell))
				}
			}
		}

	}
	return
}
