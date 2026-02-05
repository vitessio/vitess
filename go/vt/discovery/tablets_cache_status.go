package discovery

import (
	"fmt"
	"sort"

	"github.com/google/safehtml"
	"github.com/google/safehtml/template"

	"google.golang.org/protobuf/proto"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// TabletsCacheStatus is the current tablets for a cell/target.
type TabletsCacheStatus struct {
	Cell         string
	Target       *querypb.Target
	TabletsStats TabletStatsList
}

// TabletStatsList is used for sorting.
type TabletStatsList []*TabletHealth

// Len is part of sort.Interface.
func (tsl TabletStatsList) Len() int {
	return len(tsl)
}

// Less is part of sort.Interface
func (tsl TabletStatsList) Less(i, j int) bool {
	name1 := topoproto.TabletAliasString(tsl[i].Tablet.Alias)
	name2 := topoproto.TabletAliasString(tsl[j].Tablet.Alias)
	return name1 < name2
}

// Swap is part of sort.Interface
func (tsl TabletStatsList) Swap(i, j int) {
	tsl[i], tsl[j] = tsl[j], tsl[i]
}

func (tsl TabletStatsList) deepEqual(other TabletStatsList) bool {
	if len(tsl) != len(other) {
		return false
	}
	for i, th := range tsl {
		o := other[i]
		if !th.DeepEqual(o) {
			return false
		}
	}
	return true
}

// StatusAsHTML returns an HTML version of the status.
func (tcs *TabletsCacheStatus) StatusAsHTML() safehtml.HTML {
	linksTpl, err := template.New("tcs").Parse("{{ range . }}<a href=\"{{.Link}}\" style=\"{{.Color}}\">{{.Name}}</a>{{.Extra}}<br>{{ end }}")
	if err != nil {
		panic(err)
	}

	type link struct {
		Link  string
		Color safehtml.Style
		Name  string
		Extra string
	}

	var tLinks []link
	if tcs.TabletsStats != nil {
		sort.Sort(tcs.TabletsStats)
	}
	for _, ts := range tcs.TabletsStats {
		color := "green"
		extra := ""
		if ts.LastError != nil {
			color = "red"
			extra = fmt.Sprintf(" (%v)", ts.LastError)
		} else if !ts.Serving {
			color = "red"
			extra = " (Not Serving)"
		} else if ts.Target.TabletType == topodatapb.TabletType_PRIMARY {
			extra = fmt.Sprintf(" (PrimaryTermStartTime: %v)", ts.PrimaryTermStartTime)
		} else {
			extra = fmt.Sprintf(" (RepLag: %v)", ts.Stats.ReplicationLagSeconds)
		}
		name := topoproto.TabletAliasString(ts.Tablet.Alias)
		tLinks = append(tLinks, link{
			Link:  ts.getTabletDebugURL(),
			Name:  name,
			Extra: extra,
			Color: safehtml.StyleFromProperties(safehtml.StyleProperties{Color: color}),
		})
	}
	html, err := linksTpl.ExecuteToHTML(tLinks)
	if err != nil {
		panic(err)
	}
	return html
}

func (tcs *TabletsCacheStatus) deepEqual(otcs *TabletsCacheStatus) bool {
	return tcs.Cell == otcs.Cell &&
		proto.Equal(tcs.Target, otcs.Target) &&
		tcs.TabletsStats.deepEqual(otcs.TabletsStats)
}

// TabletsCacheStatusList is used for sorting.
type TabletsCacheStatusList []*TabletsCacheStatus

// Len is part of sort.Interface.
func (tcsl TabletsCacheStatusList) Len() int {
	return len(tcsl)
}

// Less is part of sort.Interface
func (tcsl TabletsCacheStatusList) Less(i, j int) bool {
	return tcsl[i].Cell+"."+tcsl[i].Target.Keyspace+"."+tcsl[i].Target.Shard+"."+string(tcsl[i].Target.TabletType) <
		tcsl[j].Cell+"."+tcsl[j].Target.Keyspace+"."+tcsl[j].Target.Shard+"."+string(tcsl[j].Target.TabletType)
}

// Swap is part of sort.Interface
func (tcsl TabletsCacheStatusList) Swap(i, j int) {
	tcsl[i], tcsl[j] = tcsl[j], tcsl[i]
}

func (tcsl TabletsCacheStatusList) deepEqual(other TabletsCacheStatusList) bool {
	if len(tcsl) != len(other) {
		return false
	}
	for i, tcs := range tcsl {
		otcs := other[i]
		if !tcs.deepEqual(otcs) {
			return false
		}
	}
	return true
}
