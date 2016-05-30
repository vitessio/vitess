// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletmanager

import (
	"flag"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/timer"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	orcAddr     = flag.String("orc_api_url", "", "Address of Orchestrator's HTTP API (e.g. http://host:port/api/). Leave empty to disable.")
	orcTimeout  = flag.Duration("orc_timeout", 30*time.Second, "Timeout for calls to Orchestrator's HTTP API")
	orcInterval = flag.Duration("orc_discover_interval", 0, "How often to ping Orchestrator's HTTP API endpoint to tell it we exist. 0 means never.")
)

// orcDiscoverLoop periodically calls orcDiscover() until process termination.
func orcDiscoverLoop(agent *ActionAgent) {
	if *orcAddr == "" || *orcInterval == 0 {
		return
	}
	apiRoot, err := url.Parse(*orcAddr)
	if err != nil {
		log.Errorf("Can't parse -orc_api_url flag value (%v): %v", *orcAddr, err)
		return
	}
	log.Infof("Starting periodic Orchestrator self-registration: API URL = %v, interval = %v", *orcAddr, *orcInterval)

	// Randomly vary the interval by +/- 25% to reduce the potential for spikes.
	ticker := timer.NewRandTicker(*orcInterval, *orcInterval/4)

	for {
		// Do the first attempt immediately.
		orcDiscover(apiRoot, agent.Tablet())
		// The only way to stop the loop is to terminate the process.
		<-ticker.C
	}
}

// orcDiscover executes a single attempt to self-register with Orchestrator.
func orcDiscover(apiRoot *url.URL, tablet *topodatapb.Tablet) {
	mysqlPort := int(tablet.PortMap["mysql"])
	if mysqlPort == 0 {
		// We don't know the MySQL port yet. Just try again later.
		return
	}

	url := *apiRoot
	url.Path = path.Join(url.Path, "discover", tablet.Hostname, strconv.Itoa(mysqlPort))
	client := &http.Client{Timeout: *orcTimeout}
	resp, err := client.Get(url.String())
	if err != nil {
		log.Infof("Orchestrator self-registration attempt failed: %v", err)
		return
	}
	// On success, we must always close resp.Body, whether we use it or not.
	resp.Body.Close()
}
