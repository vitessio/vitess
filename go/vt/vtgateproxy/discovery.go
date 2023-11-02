package vtgateproxy

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc/resolver"
)

var (
	jsonDiscoveryConfig = flag.String("json_config", "", "json file describing the host list to use fot vitess://vtgate resolution")
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
	queryOpts := target.URL.Query()
	queryParamCount := queryOpts.Get("num_connections")
	queryAZID := queryOpts.Get("az_id")
	num_connections := 0

	gateType := target.URL.Host

	if queryParamCount != "" {
		num_connections, _ = strconv.Atoi(queryParamCount)
	}

	filters := resolveFilters{
		gate_type: gateType,
	}

	if queryAZID != "" {
		filters.az_id = queryAZID
	}

	r := &resolveJSONGateConfig{
		target:          target,
		cc:              cc,
		jsonPath:        b.JsonPath,
		num_connections: num_connections,
		filters:         filters,
	}
	r.start()
	return r, nil
}
func (*JSONGateConfigDiscovery) Scheme() string { return "vtgate" }

func RegisterJsonDiscovery() {
	fmt.Printf("Registering: %v\n", *jsonDiscoveryConfig)
	resolver.Register(&JSONGateConfigDiscovery{
		JsonPath: *jsonDiscoveryConfig,
	})
}

type resolveFilters struct {
	gate_type string
	az_id     string
}

// exampleResolver is a
// Resolver(https://godoc.org/google.golang.org/grpc/resolver#Resolver).
type resolveJSONGateConfig struct {
	target          resolver.Target
	cc              resolver.ClientConn
	jsonPath        string
	ticker          *time.Ticker
	rand            *rand.Rand // safe for concurrent use.
	num_connections int
	filters         resolveFilters
}

func (r *resolveJSONGateConfig) loadConfig() (*[]resolver.Address, error) {
	config := []DiscoveryHost{}

	data, err := os.ReadFile(r.jsonPath)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, &config)
	if err != nil {
		fmt.Printf("parse err: %v\n", err)
		return nil, err
	}

	fmt.Printf("%v\n", config)

	addrs := []resolver.Address{}
	for _, s := range config {
		// Apply filters
		if r.filters.gate_type != "" {
			if r.filters.gate_type != s.Type {
				fmt.Printf("Dropped non matching type: %v\n", s.Type)
				continue
			}
		}

		if r.filters.az_id != "" {
			if r.filters.az_id != s.AZId {
				fmt.Printf("Dropped non matching az: %v\n", s.AZId)
				continue
			}
		}
		// Add matching hosts to registration list
		fmt.Printf("selected host for discovery: %v %v\n", fmt.Sprintf("%s:%s", s.NebulaAddress, s.Grpc), s)
		addrs = append(addrs, resolver.Address{Addr: fmt.Sprintf("%s:%s", s.NebulaAddress, s.Grpc)})
	}

	// Shuffle to ensure every host has a different order to iterate through
	r.rand.Shuffle(len(addrs), func(i, j int) {
		addrs[i], addrs[j] = addrs[j], addrs[i]
	})

	// Slice off the first N hosts, optionally
	if r.num_connections > 0 && r.num_connections <= len(addrs) {
		addrs = addrs[0:r.num_connections]
	}

	fmt.Printf("Returning discovery: %v\n", addrs)

	return &addrs, nil
}

func (r *resolveJSONGateConfig) start() {
	fmt.Print("Starting discovery checker\n")
	r.rand = rand.New(rand.NewSource(time.Now().UnixNano()))

	// Immediately load the initial config
	addrs, err := r.loadConfig()
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
	lastLoaded := time.Now()

	go func() {
		for range r.ticker.C {
			checkFileStat, err := os.Stat(r.jsonPath)
			isUnchanged := checkFileStat.Size() == fileStat.Size() || checkFileStat.ModTime() == fileStat.ModTime()
			isNotExpired := time.Since(lastLoaded) < 1*time.Minute
			if isUnchanged && isNotExpired {
				// no change
				continue
			}
			lastLoaded = time.Now()

			fileStat = checkFileStat
			fmt.Printf("Detected config change\n")

			addrs, err := r.loadConfig()
			if err != nil {
				// better luck next loop
				// TODO: log this
				fmt.Print("oh no\n")
				continue
			}

			r.cc.UpdateState(resolver.State{Addresses: *addrs})
		}
	}()
}
func (r *resolveJSONGateConfig) ResolveNow(o resolver.ResolveNowOptions) {}
func (r *resolveJSONGateConfig) Close() {
	r.ticker.Stop()
}

func init() {
	// Register the example ResolverBuilder. This is usually done in a package's
	// init() function.
}
