// Package resolver provides a mechanism to resolve a vtworker or vtctld
// hostname to multiple addresses. This is useful for trying to execute
// a command on all available tasks e.g. if multiple vtworker instances are
// running.
package resolver

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	log "github.com/golang/glog"
)

var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

// Resolve tries to resolve "hostname" to all available addresses ("ip:port")
// and returns them.
// From all registered Resolvers, it uses the first one whose registration key
// matches the prefix of the hostname.
// If the resolver returned no results, an error will be returned.
// If no resolver did match, the hostname will be returned as the only address.
// If multiple addresses are returned, the list will be shuffled.
func Resolve(hostname string) ([]string, error) {
	var addrs []string

	for prefix, resolver := range resolvers {
		if strings.HasPrefix(hostname, prefix) {
			var err error
			addrs, err = resolver.Resolve(hostname)
			if err != nil {
				return nil, err
			}
			if len(addrs) == 0 {
				return nil, fmt.Errorf("resolver for prefix: %q returned no addresses", prefix)
			}
			break
		}
	}

	if len(addrs) == 0 {
		addrs = append(addrs, hostname)
	}

	if len(addrs) > 1 {
		shuffle(addrs)
	}

	return addrs, nil
}

// shuffle uses the Fisher-Yates shuffle algorithm to shuffle "list".
func shuffle(list []string) {
	for i := len(list) - 1; i >= 1; i-- {
		j := rng.Intn(i + 1)
		list[i], list[j] = list[j], list[i]
	}
}

// resolvers is the list of registered resolvers per matching prefix.
// Only if the hostname matches the prefix key, the resolver will be used.
var resolvers = make(map[string]Resolver)

// Resolver can be implemented to resolve a "hostname" to all available
// addresses (list of "ip:port" strings).
type Resolver interface {
	Resolve(hostname string) ([]string, error)
}

// Register will register "resolver" for all hostnames matching "prefix".
func Register(prefix string, resolver Resolver) {
	if _, ok := resolvers[prefix]; ok {
		log.Fatalf("Resolver for prefix: %q already exists", prefix)
	}
	resolvers[prefix] = resolver
}
