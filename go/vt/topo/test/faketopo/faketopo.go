// faketopo contains utitlities for tests that have to interact with a
// Vitess topology.
package faketopo

import (
	"fmt"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletmanager"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/zktopo"
)

const (
	TestShard    = "-80"
	TestKeyspace = "test_keyspace"
)

func newKeyRange(value string) key.KeyRange {
	_, result, err := topo.ValidateShardName(value)
	if err != nil {
		panic(err)
	}
	return result
}

type tabletPack struct {
	*topo.Tablet
	mysql *mysqlctl.FakeMysqlDaemon
}

// Fixture is a fixture that provides a fresh topology, to which you
// can add tablets that react to events and have fake MySQL
// daemons. It uses an in memory fake ZooKeeper to store its
// data. When you are done with the fixture you have to call its
// TearDown method.
type Fixture struct {
	*testing.T
	tablets  map[int]*tabletPack
	done     chan struct{}
	Topo     topo.Server
	Wrangler *wrangler.Wrangler
}

// New creates a topology fixture.
func New(t *testing.T, cells []string) *Fixture {
	ts := zktopo.NewTestServer(t, cells)

	wr := wrangler.New(ts, 1*time.Second, 1*time.Second)
	wr.UseRPCs = false

	return &Fixture{
		T:        t,
		Topo:     ts,
		Wrangler: wr,
		done:     make(chan struct{}, 1),
		tablets:  make(map[int]*tabletPack),
	}
}

// TearDown releases any resources used by the fixture.
func (fix *Fixture) TearDown() {
	close(fix.done)
}

// startFakeTabletActionLoop will start the action loop for a fake
// tablet.
func (fix *Fixture) startFakeTabletActionLoop(tablet *tabletPack) {
	go func() {
		f := func(actionPath, data string) error {
			actionNode, err := actionnode.ActionNodeFromJson(data, actionPath)
			if err != nil {
				fix.Fatalf("ActionNodeFromJson failed: %v\n%v", err, data)
			}

			ta := tabletmanager.NewTabletActor(nil, tablet.mysql, fix.Topo, tablet.Alias)
			if err := ta.HandleAction(actionPath, actionNode.Action, actionNode.ActionGuid, false); err != nil {
				// action may just fail for any good reason
				fix.Logf("HandleAction failed for %v: %v", actionNode.Action, err)
			}
			return nil
		}
		fix.Topo.ActionEventLoop(tablet.Alias, f, fix.done)
	}()
}

// MakeMySQLMaster makes the (fake) MySQL used by tablet identified by
// uid the master.
func (fix *Fixture) MakeMySQLMaster(uid int) {
	newMaster, ok := fix.tablets[uid]
	if !ok {
		fix.Fatalf("bad tablet uid: %v", uid)
	}
	for id, tablet := range fix.tablets {
		if id == uid {
			tablet.mysql.MasterAddr = ""
		} else {
			tablet.mysql.MasterAddr = newMaster.GetMysqlIpAddr()
		}
	}
}

// AddTablet adds a new tablet to the topology and starts its event
// loop.
func (fix *Fixture) AddTablet(uid int, cell string, tabletType topo.TabletType, master *topo.Tablet) *topo.Tablet {
	tablet := &topo.Tablet{
		Alias:    topo.TabletAlias{Cell: cell, Uid: uint32(uid)},
		Hostname: fmt.Sprintf("%vbsr%v", cell, uid),
		IPAddr:   fmt.Sprintf("212.244.218.%v", uid),
		Portmap: map[string]int{
			"vt":    3333 + 10*uid,
			"mysql": 3334 + 10*uid,
		},
		Keyspace: TestKeyspace,
		Type:     tabletType,
		Shard:    TestShard,
		KeyRange: newKeyRange(TestShard),
	}
	if master != nil {
		tablet.Parent = master.Alias
	}

	if err := fix.Wrangler.InitTablet(tablet, true, true, false); err != nil {
		fix.Fatalf("CreateTablet: %v", err)
	}
	mysqlDaemon := &mysqlctl.FakeMysqlDaemon{}
	if master != nil {
		mysqlDaemon.MasterAddr = master.GetMysqlIpAddr()
	}
	mysqlDaemon.MysqlPort = 3334 + 10*uid

	pack := &tabletPack{Tablet: tablet, mysql: mysqlDaemon}
	fix.startFakeTabletActionLoop(pack)

	fix.tablets[uid] = pack

	return tablet
}

// GetTablet returns a fresh copy of the tablet identified by uid.
func (fix *Fixture) GetTablet(uid int) *topo.TabletInfo {
	tablet, ok := fix.tablets[uid]
	if !ok {
		panic("bad tablet uid")
	}
	ti, err := fix.Topo.GetTablet(tablet.Alias)
	if err != nil {
		fix.Fatalf("GetTablet %v: %v", tablet.Alias, err)
	}
	return ti

}
