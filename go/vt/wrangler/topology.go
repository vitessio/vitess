package wrangler

import (
	"net"
	"strings"

	"code.google.com/p/vitess/go/relog"
	tm "code.google.com/p/vitess/go/vt/tabletmanager"
)

// ShardNodes represents all tablet nodes for a shard. The keys are
// string representations of tablet types.
type ShardNodes map[string][]*TabletNode

// KeyspaceNodes represents all tablet nodes in a keyspace.
type KeyspaceNodes map[string]ShardNodes

// TabletTypes returns a slice of tablet type names this ks
// contains.
func (ks KeyspaceNodes) TabletTypes() []string {
	contained := make([]string, 0)
	for _, t := range tm.AllTabletTypes {
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
	*tm.TabletInfo
	IsReplicating bool
}

func (t *TabletNode) ShortName() string {
	return strings.SplitN(t.Addr, ".", 2)[0]
}

func (t *TabletNode) IsReplicationOk() bool {
	return !t.IsSlaveType() || t.IsReplicating
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

type shardAddrPair struct {
	shard, addr string
}

func (wr *Wrangler) DbTopology() (*Topology, error) {
	tabletInfos, err := GetAllTabletsAccrossCells(wr.zconn)
	if err != nil {
		return nil, err
	}
	topology := NewTopology()
	masters := make([]*TabletNode, 0)
	slaves := make(map[shardAddrPair]*TabletNode)

	for _, ti := range tabletInfos {
		tablet := &TabletNode{TabletInfo: ti}
		switch tablet.Type {
		case tm.TYPE_IDLE:
			topology.Idle = append(topology.Idle, tablet)
		case tm.TYPE_SCRAP:
			topology.Scrap = append(topology.Scrap, tablet)
		default:
			switch tablet.Type {
			case tm.TYPE_MASTER:
				masters = append(masters, tablet)
			case tm.TYPE_REPLICA:
				host, _, err := net.SplitHostPort(tablet.MysqlIpAddr)
				if err != nil {
					return nil, err
				}
				slaves[shardAddrPair{tablet.Shard, host}] = tablet
			}
			if _, ok := topology.Assigned[tablet.Keyspace]; !ok {
				topology.Assigned[tablet.Keyspace] = make(map[string]ShardNodes)
			}
			if _, ok := topology.Assigned[tablet.Keyspace][tablet.Shard]; !ok {
				topology.Assigned[tablet.Keyspace][tablet.Shard] = make(ShardNodes)
			}

			topology.Assigned[tablet.Keyspace][tablet.Shard][string(tablet.Type)] = append(topology.Assigned[tablet.Keyspace][tablet.Shard][string(tablet.Type)], tablet)
		}

	}
	errors := make(chan error)
	results := make(chan []shardAddrPair)
	for _, master := range masters {
		go func(master *TabletNode) {
			actionPath, err := wr.ai.GetSlaves(master.Path())
			if err != nil {
				errors <- err
				return
			}
			slaveList, err := wr.ai.WaitForCompletionReply(actionPath, -1)
			if err != nil {
				errors <- err
				return
			}
			sl := slaveList.(*tm.SlaveList)
			newSlaves := make([]shardAddrPair, len(sl.Addrs))
			for i, addr := range sl.Addrs {
				newSlaves[i] = shardAddrPair{master.Shard, addr}
			}
			results <- newSlaves
		}(master)
	}
	for _ = range masters {
		select {
		case newSlaves := <-results:
			for _, key := range newSlaves {
				slave, ok := slaves[key]
				if !ok {
					relog.Error("slave not present in Zookeeper returned by GetSlaves: %v (shard %v)", key.addr, key.shard)
					continue
				}
				slave.IsReplicating = true

			}
		case err := <-errors:
			return nil, err
		}
	}
	return topology, nil
}
