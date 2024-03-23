/*
Copyright 2024 The Vitess Authors.

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
package vtgateproxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"

	"google.golang.org/grpc/resolver"

	"vitess.io/vitess/go/vt/log"
)

var (
	jsonDiscoveryConfig = flag.String("json_config", "", "json file describing the host list to use fot vitess://vtgate resolution")
	addressField        = flag.String("address_field", "address", "field name in the json file containing the address")
	portField           = flag.String("port_field", "port", "field name in the json file containing the port")
	numConnections      = flag.Int("num_connections", 4, "number of outbound GPRC connections to maintain")
)

// File based discovery for vtgate grpc endpoints
// This loads the list of hosts from json and watches for changes to the list of hosts. It will select N connection to maintain to backend vtgates.
// Connections will rebalance every 5 minutes
//
// Example json config - based on the slack hosts format
//
// [
//     {
//         "address": "10.4.56.194",
//         "az_id": "use1-az1",
//         "grpc": "15999",
//         "type": "aux"
//     },
//
// Naming scheme:
// vtgate://<type>?num_connections=<int>&az_id=<string>
//
// num_connections: Option number of hosts to open connections to for round-robin selection
// az_id: Filter to just hosts in this az (optional)
// type: Only select from hosts of this type (required)
//

type DiscoveryHost struct {
	Address       string
	NebulaAddress string `json:"nebula_address"`
	Grpc          string
	AZId          string `json:"az_id"`
	Type          string
}

type JSONGateConfigDiscovery struct {
	JsonPath string
}

func (b *JSONGateConfigDiscovery) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	attrs := target.URL.Query()

	// If the config specifies a pool type attribute, then the caller must supply it in the connection
	// attributes, otherwise reject the request.
	poolType := ""
	if *poolTypeAttr != "" {
		poolType = attrs.Get(*poolTypeAttr)
		if poolType == "" {
			return nil, fmt.Errorf("pool type attribute %s not in target", *poolTypeAttr)
		}
	}

	// Affinity on the other hand is just an optimization
	affinity := ""
	if *affinityAttr != "" {
		affinity = attrs.Get(*affinityAttr)
	}

	log.V(100).Infof("Start discovery for target %v poolType %s affinity %s\n", target.URL.String(), poolType, affinity)

	r := &JSONGateConfigResolver{
		target:   target,
		cc:       cc,
		jsonPath: b.JsonPath,
		poolType: poolType,
		affinity: affinity,
	}
	r.start()
	return r, nil
}
func (*JSONGateConfigDiscovery) Scheme() string { return "vtgate" }

func RegisterJsonDiscovery() {
	jsonDiscovery := &JSONGateConfigDiscovery{
		JsonPath: *jsonDiscoveryConfig,
	}
	resolver.Register(jsonDiscovery)
	log.Infof("Registered JSON discovery scheme %v to watch: %v\n", jsonDiscovery.Scheme(), *jsonDiscoveryConfig)
}

// Resolver(https://godoc.org/google.golang.org/grpc/resolver#Resolver).
type JSONGateConfigResolver struct {
	target   resolver.Target
	cc       resolver.ClientConn
	jsonPath string
	poolType string
	affinity string

	ticker *time.Ticker
	rand   *rand.Rand // safe for concurrent use.
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func jsonDump(data interface{}) string {
	json, _ := json.Marshal(data)
	return string(json)
}

func (r *JSONGateConfigResolver) resolve() (*[]resolver.Address, []byte, error) {

	log.V(100).Infof("resolving target %s to %d connections\n", r.target.URL.String(), *numConnections)

	data, err := os.ReadFile(r.jsonPath)
	if err != nil {
		return nil, nil, err
	}

	hosts := []map[string]interface{}{}
	err = json.Unmarshal(data, &hosts)
	if err != nil {
		log.Errorf("error parsing JSON discovery file %s: %v\n", r.jsonPath, err)
		return nil, nil, err
	}

	// optionally filter to only hosts that match the pool type
	if r.poolType != "" {
		candidates := []map[string]interface{}{}
		for _, host := range hosts {
			hostType, ok := host[*poolTypeAttr]
			if ok && hostType == r.poolType {
				candidates = append(candidates, host)
				log.V(1000).Infof("matched host %s with type %s", jsonDump(host), hostType)
			} else {
				log.V(1000).Infof("skipping host %s with type %s", jsonDump(host), hostType)
			}
		}
		hosts = candidates
	}

	// Shuffle to ensure every host has a different order to iterate through
	r.rand.Shuffle(len(hosts), func(i, j int) {
		hosts[i], hosts[j] = hosts[j], hosts[i]
	})

	// If affinity is specified, then shuffle those hosts to the front
	if r.affinity != "" {
		i := 0
		for j := 0; j < len(hosts); j++ {
			hostAffinity, ok := hosts[j][*affinityAttr]
			if ok && hostAffinity == r.affinity {
				hosts[i], hosts[j] = hosts[j], hosts[i]
				i++
			}
		}
	}

	// Grab the first N addresses, and voila!
	var addrs []resolver.Address
	hosts = hosts[:min(*numConnections, len(hosts))]
	for _, host := range hosts {
		addrs = append(addrs, resolver.Address{
			Addr: fmt.Sprintf("%s:%s", host[*addressField], host[*portField]),
		})
	}

	h := sha256.New()
	if _, err := io.Copy(h, bytes.NewReader(data)); err != nil {
		return nil, nil, err
	}
	sum := h.Sum(nil)

	log.V(100).Infof("resolved %s to hosts %s addrs: 0x%x, %v\n", r.target.URL.String(), jsonDump(hosts), sum, addrs)

	return &addrs, sum, nil
}

func (r *JSONGateConfigResolver) start() {
	log.V(100).Infof("Starting discovery checker\n")
	r.rand = rand.New(rand.NewSource(time.Now().UnixNano()))

	// Immediately load the initial config
	addrs, hash, err := r.resolve()
	if err == nil {
		// if we parse ok, populate the local address store
		r.cc.UpdateState(resolver.State{Addresses: *addrs})
	}

	// Start a config watcher
	r.ticker = time.NewTicker(100 * time.Millisecond)
	fileStat, err := os.Stat(r.jsonPath)
	if err != nil {
		return
	}
	go func() {
		for range r.ticker.C {
			checkFileStat, err := os.Stat(r.jsonPath)
			if err != nil {
				log.Errorf("Error stat'ing config %v\n", err)
				continue
			}
			isUnchanged := checkFileStat.Size() == fileStat.Size() || checkFileStat.ModTime() == fileStat.ModTime()
			if isUnchanged {
				// no change
				continue
			}

			fileStat = checkFileStat
			log.V(100).Infof("Detected config change\n")

			addrs, newHash, err := r.resolve()
			if err != nil {
				// better luck next loop
				// TODO: log this
				log.Errorf("Error resolving config: %v\n", err)
				continue
			}

			// Make sure this wasn't a spurious change by checking the hash
			if bytes.Equal(hash, newHash) && newHash != nil {
				log.V(100).Infof("No content changed in discovery file... ignoring\n")
				continue
			}

			hash = newHash

			r.cc.UpdateState(resolver.State{Addresses: *addrs})
		}
	}()

	log.V(100).Infof("Loaded hosts, starting ticker\n")

}
func (r *JSONGateConfigResolver) ResolveNow(o resolver.ResolveNowOptions) {}
func (r *JSONGateConfigResolver) Close() {
	r.ticker.Stop()
}

func init() {
	// Register the example ResolverBuilder. This is usually done in a package's
	// init() function.
}
