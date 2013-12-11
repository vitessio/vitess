package wrangler

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
)

// TabletNodesByType maps tablet types to slices of tablet nodes.
type TabletNodesByType map[string][]*TabletNode

// ShardNodes represents all tablet nodes for a shard. The keys are
// string representations of tablet types.
type ShardNodes struct {
	TabletNodes TabletNodesByType
	Name        string
}

type numericShardNodesList []*ShardNodes

func (nsnl numericShardNodesList) Len() int {
	return len(nsnl)
}

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

func (nsnl numericShardNodesList) Swap(i, j int) {
	nsnl[i], nsnl[j] = nsnl[j], nsnl[i]
}

type rangeShardNodesList []*ShardNodes

func (rsnl rangeShardNodesList) Len() int {
	return len(rsnl)
}

func (rsnl rangeShardNodesList) Less(i, j int) bool {
	return rsnl[i].Name < rsnl[j].Name
}

func (rsnl rangeShardNodesList) Swap(i, j int) {
	rsnl[i], rsnl[j] = rsnl[j], rsnl[i]
}

// KeyspaceNodes represents all tablet nodes in a keyspace.
type KeyspaceNodes map[string]TabletNodesByType

func (ks KeyspaceNodes) hasOnlyNumericShardNames() bool {
	for name := range ks {
		if _, err := strconv.Atoi(name); err != nil {
			return false
		}
	}
	return true

}

// ShardNodes returns all the shard nodes, in a reasonable order.
func (ks KeyspaceNodes) ShardNodes() []*ShardNodes {
	result := make([]*ShardNodes, len(ks))
	i := 0
	for name, snm := range ks {
		result[i] = &ShardNodes{Name: name, TabletNodes: snm}
		i++
	}

	if ks.hasOnlyNumericShardNames() {
		sort.Sort(numericShardNodesList(result))
	} else {
		sort.Sort(rangeShardNodesList(result))
	}

	return result
}

// TabletTypes returns a slice of tablet type names this ks
// contains.
func (ks KeyspaceNodes) TabletTypes() []string {
	contained := make([]string, 0)
	for _, t := range topo.AllTabletTypes {
		name := string(t)
		if ks.HasType(name) {
			contained = append(contained, name)
		}
	}
	return contained
}

// HasType returns true if ks has any tablets with the named type.
func (ks KeyspaceNodes) HasType(name string) bool {
	for _, shard := range ks {
		if _, ok := shard[name]; ok {
			return true
		}
	}
	return false
}

// TabletNode is the representation of a tablet in the db topology.
type TabletNode struct {
	Host  string
	Alias topo.TabletAlias
	Port  int
}

func (tn TabletNode) ShortName() string {
	hostPart := strings.SplitN(tn.Host, ".", 2)[0]
	if tn.Port == 0 {
		return hostPart
	}
	return fmt.Sprintf("%v:%v", hostPart, tn.Port)
}

func TabletNodeFromTabletInfo(ti *topo.TabletInfo) (*TabletNode, error) {
	if err := ti.ValidatePortmap(); err != nil {
		log.Errorf("ValidatePortmap(%v): %v", ti.Alias, err)
	}
	return &TabletNode{
		Host:  ti.Hostname,
		Port:  ti.Portmap["vt"],
		Alias: ti.Alias,
	}, nil
}

func TabletNodeFromEndPoint(ep topo.EndPoint, cell string) *TabletNode {
	return &TabletNode{
		Host: ep.Host,
		Alias: topo.TabletAlias{
			Uid:  ep.Uid,
			Cell: cell},
		Port: ep.NamedPortMap[topo.DefaultPortName],
	}

}

type Topology struct {
	Assigned map[string]KeyspaceNodes
	Idle     []*TabletNode
	Scrap    []*TabletNode
	Partial  bool
}

func NewTopology() *Topology {
	return &Topology{
		Assigned: make(map[string]KeyspaceNodes),
		Idle:     make([]*TabletNode, 0),
		Scrap:    make([]*TabletNode, 0),
		Partial:  false,
	}
}

func (wr *Wrangler) DbTopology() (*Topology, error) {
	topology := NewTopology()
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

	for _, ti := range tabletInfos {
		tablet, err := TabletNodeFromTabletInfo(ti)
		if err != nil {
			return nil, err
		}
		switch ti.Type {
		case topo.TYPE_IDLE:
			topology.Idle = append(topology.Idle, tablet)
		case topo.TYPE_SCRAP:
			topology.Scrap = append(topology.Scrap, tablet)
		default:
			if _, ok := topology.Assigned[ti.Keyspace]; !ok {
				topology.Assigned[ti.Keyspace] = make(map[string]TabletNodesByType)
			}
			if _, ok := topology.Assigned[ti.Keyspace][ti.Shard]; !ok {
				topology.Assigned[ti.Keyspace][ti.Shard] = make(TabletNodesByType)
			}
			topology.Assigned[ti.Keyspace][ti.Shard][string(ti.Type)] = append(topology.Assigned[ti.Keyspace][ti.Shard][string(ti.Type)], tablet)
		}

	}
	return topology, nil
}

type ServingGraph struct {
	Keyspaces map[string]KeyspaceNodes
	Cell      string
}

func (wr *Wrangler) ServingGraph(cell string) (*ServingGraph, error) {
	servingGraph := &ServingGraph{
		Cell:      cell,
		Keyspaces: make(map[string]KeyspaceNodes)}

	keyspaces, err := wr.ts.GetSrvKeyspaceNames(cell)
	if err != nil {
		return nil, err
	}
	for _, keyspace := range keyspaces {
		servingGraph.Keyspaces[keyspace] = make(map[string]TabletNodesByType)

		shards, err := wr.ts.GetShardNames(keyspace)
		if err != nil {
			return nil, err
		}
		for _, shard := range shards {
			servingGraph.Keyspaces[keyspace][shard] = make(TabletNodesByType)
			tabletTypes, err := wr.ts.GetSrvTabletTypesPerShard(cell, keyspace, shard)
			if err != nil {
				return nil, err
			}
			for _, tabletType := range tabletTypes {
				endPoints, err := wr.ts.GetEndPoints(cell, keyspace, shard, tabletType)
				if err != nil {
					return nil, err
				}
				for _, endPoint := range endPoints.Entries {
					servingGraph.Keyspaces[keyspace][shard][string(tabletType)] = append(servingGraph.Keyspaces[keyspace][shard][string(tabletType)], TabletNodeFromEndPoint(endPoint, cell))
				}
			}
		}

	}
	return servingGraph, nil
}
