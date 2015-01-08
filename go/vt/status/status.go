// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package status defines a few useful functions for our binaries,
// mainly to link the status page with a vtctld instance.
package status

import (
	"flag"
	"fmt"
	"html/template"
	"net/url"
	"strings"

	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	vtctldAddr = flag.String("vtctld_addr", "", "address of a vtctld instance")
	_          = flag.String("vtctld_topo_explorer", "", "this flag is no longer used")
)

// MakeVtctldRedirect returns an absolute vtctld url that will
// redirect to the page for the topology object specified in q.
func MakeVtctldRedirect(text string, q map[string]string) template.HTML {
	query := url.Values{}
	for k, v := range q {
		query.Set(k, v)
	}
	url := "/explorers/redirect" + "?" + query.Encode()
	return VtctldLink(text, url)
}

// VtctldLink returns the HTML to display a link to the fully
// qualified vtctld url whose path is given as parameter.
// If no vtctld_addr flag was passed in, we just return the text with no link.
func VtctldLink(text, urlPath string) template.HTML {
	if *vtctldAddr == "" {
		return template.HTML(text)
	}
	var fullURL string
	if strings.HasSuffix(*vtctldAddr, "/") {
		fullURL = *vtctldAddr + urlPath
	} else {
		fullURL = *vtctldAddr + "/" + urlPath
	}

	return template.HTML(fmt.Sprintf(`<a href="%v">%v</a>`, fullURL, text))
}

// VtctldKeyspace returns the keyspace name, possibly linked to the
// keyspace page in vtctld.
func VtctldKeyspace(keyspace string) template.HTML {
	return MakeVtctldRedirect(keyspace,
		map[string]string{
			"type":     "keyspace",
			"keyspace": keyspace,
		})
}

// VtctldShard returns the shard name, possibly linked to the shard
// page in vtctld.
func VtctldShard(keyspace, shard string) template.HTML {
	return MakeVtctldRedirect(shard, map[string]string{
		"type":     "shard",
		"keyspace": keyspace,
		"shard":    shard,
	})
}

// VtctldSrvCell returns the cell name, possibly linked to the
// serving graph page in vtctld for that page.
func VtctldSrvCell(cell string) template.HTML {
	return VtctldLink(cell, "/serving_graph/"+cell)
}

// VtctldSrvKeyspace returns the keyspace name, possibly linked to the
// SrvKeyspace page in vtctld.
func VtctldSrvKeyspace(cell, keyspace string) template.HTML {
	return MakeVtctldRedirect(keyspace, map[string]string{
		"type":     "srv_keyspace",
		"cell":     cell,
		"keyspace": keyspace,
	})
}

// VtctldSrvShard returns the shard name, possibly linked to the
// SrvShard page in vtctld.
func VtctldSrvShard(cell, keyspace, shard string) template.HTML {
	return MakeVtctldRedirect(shard, map[string]string{
		"type":     "srv_shard",
		"cell":     cell,
		"keyspace": keyspace,
		"shard":    shard,
	})
}

// VtctldSrvType returns the tablet type, possibly linked to the
// EndPoints page in vtctld.
func VtctldSrvType(cell, keyspace, shard string, tabletType topo.TabletType) template.HTML {
	if !topo.IsInServingGraph(tabletType) {
		return template.HTML(tabletType)
	}
	return MakeVtctldRedirect(string(tabletType), map[string]string{
		"type":        "srv_type",
		"cell":        cell,
		"keyspace":    keyspace,
		"shard":       shard,
		"tablet_type": string(tabletType),
	})
}

// VtctldReplication returns 'cell/keyspace/shard', possibly linked to the
// ShardReplication page in vtctld.
func VtctldReplication(cell, keyspace, shard string) template.HTML {
	return MakeVtctldRedirect(fmt.Sprintf("%v/%v/%v", cell, keyspace, shard),
		map[string]string{
			"type":     "replication",
			"keyspace": keyspace,
			"shard":    shard,
			"cell":     cell,
		})
}

// VtctldTablet returns the tablet alias, possibly linked to the
// Tablet page in vtctld.
func VtctldTablet(aliasName string) template.HTML {
	return MakeVtctldRedirect(aliasName, map[string]string{
		"type":  "tablet",
		"alias": aliasName,
	})
}

func init() {
	servenv.AddStatusFuncs(template.FuncMap{
		"github_com_youtube_vitess_vtctld_keyspace":     VtctldKeyspace,
		"github_com_youtube_vitess_vtctld_shard":        VtctldShard,
		"github_com_youtube_vitess_vtctld_srv_cell":     VtctldSrvCell,
		"github_com_youtube_vitess_vtctld_srv_keyspace": VtctldSrvKeyspace,
		"github_com_youtube_vitess_vtctld_srv_shard":    VtctldSrvShard,
		"github_com_youtube_vitess_vtctld_srv_type":     VtctldSrvType,
		"github_com_youtube_vitess_vtctld_replication":  VtctldReplication,
		"github_com_youtube_vitess_vtctld_tablet":       VtctldTablet,
	})
}
