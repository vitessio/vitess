/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tabletmanager

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"time"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

var (
	orcAddr     = flag.String("orc_api_url", "", "Address of Orchestrator's HTTP API (e.g. http://host:port/api/). Leave empty to disable Orchestrator integration.")
	orcTimeout  = flag.Duration("orc_timeout", 30*time.Second, "Timeout for calls to Orchestrator's HTTP API")
	orcInterval = flag.Duration("orc_discover_interval", 0, "How often to ping Orchestrator's HTTP API endpoint to tell it we exist. 0 means never.")
)

type orcClient struct {
	apiRoot    *url.URL
	httpClient *http.Client
}

// newOrcClient creates a client for the Orchestrator HTTP API.
// It should only be called after flags have been parsed.
func newOrcClient() (*orcClient, error) {
	if *orcAddr == "" {
		// Orchestrator integration is disabled.
		return nil, nil
	}
	apiRoot, err := url.Parse(*orcAddr)
	if err != nil {
		return nil, vterrors.Wrapf(err, "can't parse -orc_api_url flag value (%v)", *orcAddr)
	}
	return &orcClient{
		apiRoot:    apiRoot,
		httpClient: &http.Client{Timeout: *orcTimeout},
	}, nil
}

// DiscoverLoop periodically calls orc.discover() until process termination.
// The Tablet is read from the given agent each time before calling discover().
// Usually this will be launched as a background goroutine.
func (orc *orcClient) DiscoverLoop(agent *ActionAgent) {
	if *orcInterval == 0 {
		// 0 means never.
		return
	}
	log.Infof("Starting periodic Orchestrator self-registration: API URL = %v, interval = %v", *orcAddr, *orcInterval)

	// Randomly vary the interval by +/- 25% to reduce the potential for spikes.
	ticker := timer.NewRandTicker(*orcInterval, *orcInterval/4)

	// Remember whether we've most recently succeeded or failed.
	var lastErr error

	for {
		// Do the first attempt immediately.
		err := orc.Discover(agent.Tablet())

		// Only log if we're transitioning between success and failure states.
		if (err != nil) != (lastErr != nil) {
			if err != nil {
				log.Warningf("Orchestrator self-registration attempt failed: %v", err)
			} else {
				log.Infof("Orchestrator self-registration succeeded.")
			}
		}
		lastErr = err

		// Wait for the next tick.
		// The only way to stop the loop is to terminate the process.
		<-ticker.C
	}
}

// Discover executes a single attempt to self-register with Orchestrator.
func (orc *orcClient) Discover(tablet *topodatapb.Tablet) error {
	host, port, err := mysqlHostPort(tablet)
	if err != nil {
		return err
	}
	_, err = orc.apiGet("discover", host, port)
	return err
}

// BeginMaintenance tells Orchestrator not to touch the given tablet
// until we call EndMaintenance().
func (orc *orcClient) BeginMaintenance(tablet *topodatapb.Tablet, reason string) error {
	host, port, err := mysqlHostPort(tablet)
	if err != nil {
		return err
	}
	_, err = orc.apiGet("begin-maintenance", host, port, "vitess", reason)
	return err
}

// EndMaintenance tells Orchestrator to remove the maintenance block on the
// given tablet, which should have been placed there by BeginMaintenance().
func (orc *orcClient) EndMaintenance(tablet *topodatapb.Tablet) error {
	host, port, err := mysqlHostPort(tablet)
	if err != nil {
		return err
	}
	_, err = orc.apiGet("end-maintenance", host, port)
	return err
}

func (orc *orcClient) InActiveShardRecovery(tablet *topodatapb.Tablet) (bool, error) {
	alias := fmt.Sprintf("%v.%v", tablet.GetKeyspace(), tablet.GetShard())

	// TODO(zmagg): Replace this with simpler call to active-cluster-recovery
	// when call with alias parameter is supported.
	resp, err := orc.apiGet("audit-recovery", "alias", alias)

	if err != nil {
		return false, err
	}

	var r []map[string]interface{}

	if err := json.Unmarshal(resp, &r); err != nil {
		return false, err
	}

	// Orchestrator returns a 0-length response when it has no history of recovery on this cluster.
	if len(r) == 0 {
		return false, nil
	}

	active, ok := r[0]["IsActive"].(bool)

	if !ok {
		return false, fmt.Errorf("Error parsing JSON response from Orchestrator")
	}
	return active, nil
}

func mysqlHostPort(tablet *topodatapb.Tablet) (host, port string, err error) {
	mysqlPort := int(topoproto.MysqlPort(tablet))
	if mysqlPort == 0 {
		return "", "", fmt.Errorf("MySQL port is unknown for tablet %v (mysqld may not be running yet)", topoproto.TabletAliasString(tablet.Alias))
	}
	return topoproto.MysqlHostname(tablet), strconv.Itoa(mysqlPort), nil
}

// apiGet calls the given Orchestrator API endpoint.
// The final, assembled path will be URL-escaped, but things like '/' inside a
// path part can still confuse the HTTP API. We can't do anything about that
// because Orchestrator's API chose to put variable values in path elements
// rather than query arguments.
func (orc *orcClient) apiGet(pathParts ...string) ([]byte, error) {
	// Append pathParts to a copy of the apiRoot URL.
	url := *orc.apiRoot
	fullPath := make([]string, 0, len(pathParts)+1)
	fullPath = append(fullPath, url.Path)
	fullPath = append(fullPath, pathParts...)
	url.Path = path.Join(fullPath...)

	// Note that url.String() will URL-escape the path we gave it above.
	resp, err := orc.httpClient.Get(url.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}
