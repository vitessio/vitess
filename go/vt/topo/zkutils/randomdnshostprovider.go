package zkutils

import (
	"math/rand"
	"net"
	"sync"
	"time"
)

// RandomDNSHostProvider is a modified version of SimpleDNSHostProvider, with the only difference being that
// the resolveAddr method returns a random entry instead of the first one.
type RandomDNSHostProvider struct {
	mu         sync.Mutex // Protects everything, so we can add asynchronous updates later.
	servers    []string
	curr       int
	attempt    int
	lookupHost func(string) ([]string, error) // Override of net.LookupHost, for testing.
}

// Init is called first, with the servers specified in the connection
// string. It uses DNS to look up addresses for each server on demand.
// It assumes that each DNS entry resolves to exactly one address.
func (hp *RandomDNSHostProvider) Init(servers []string) error {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	hp.servers = append([]string{}, servers...)
	// Randomize the order of the servers to avoid creating hotspots
	hp.curr = rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(hp.servers))
	return nil
}

func (hp *RandomDNSHostProvider) resolveAddr(addr string) (string, error) {
	lookupHost := hp.lookupHost
	if lookupHost == nil {
		lookupHost = net.LookupHost
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	addrs, err := lookupHost(host)
	if err != nil {
		return "", err
	}

	// Given that we query DNS every time to give a list of addresses, it makes no sense to iterate over indexes
	// of any sort, as the ordering and/or results might be different with every resolution attempt.
	// Therefore, we just return the random address instead.
	randomIndex := rand.Intn(len(addrs))
	return net.JoinHostPort(addrs[randomIndex], port), nil
}

// Next returns the next server to connect to. retryStart will be true
// if we've looped through all known servers without Connected() being
// called.
func (hp *RandomDNSHostProvider) Next() (server string, retryStart bool) {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	hp.curr = (hp.curr + 1) % len(hp.servers)
	hp.attempt++
	if hp.attempt > len(hp.servers) {
		retryStart = true
		hp.attempt = 1
	}
	addr, err := hp.resolveAddr(hp.servers[hp.curr])
	if err != nil {
		// There isn't a better way to handle an error in the current API,
		// so return an empty address to fast fail in connect.
		return "", retryStart
	}
	return addr, retryStart
}

// Connected notifies the HostProvider of a successful connection.
func (hp *RandomDNSHostProvider) Connected() {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	hp.attempt = 1
}

// Len returns the number of servers available
func (hp *RandomDNSHostProvider) Len() int {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	return len(hp.servers)
}
