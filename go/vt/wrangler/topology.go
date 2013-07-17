package wrangler

import (
	"sort"
	"strconv"
	"strings"

	"code.google.com/p/vitess/go/vt/topo"
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
	*topo.TabletInfo
}

func (t *TabletNode) ShortName() string {
	return strings.SplitN(t.Addr, ".", 2)[0]
}

type Topology struct {
	Assigned map[string]KeyspaceNodes
	Idle     []*TabletNode
	Scrap    []*TabletNode
}

func NewTopology() *Topology {
	return &Topology{
		Assigned: make(map[string]KeyspaceNodes),
		Idle:     make([]*TabletNode, 0),
		Scrap:    make([]*TabletNode, 0),
	}
}

func (wr *Wrangler) DbTopology() (*Topology, error) {
	tabletInfos, err := GetAllTabletsAccrossCells(wr.ts)
	if err != nil {
		return nil, err
	}
	topology := NewTopology()

	for _, ti := range tabletInfos {
		tablet := &TabletNode{TabletInfo: ti}
		switch tablet.Type {
		case topo.TYPE_IDLE:
			topology.Idle = append(topology.Idle, tablet)
		case topo.TYPE_SCRAP:
			topology.Scrap = append(topology.Scrap, tablet)
		default:
			if _, ok := topology.Assigned[tablet.Keyspace]; !ok {
				topology.Assigned[tablet.Keyspace] = make(map[string]TabletNodesByType)
			}
			if _, ok := topology.Assigned[tablet.Keyspace][tablet.Shard]; !ok {
				topology.Assigned[tablet.Keyspace][tablet.Shard] = make(TabletNodesByType)
			}
			topology.Assigned[tablet.Keyspace][tablet.Shard][string(tablet.Type)] = append(topology.Assigned[tablet.Keyspace][tablet.Shard][string(tablet.Type)], tablet)
		}

	}
	return topology, nil
}
