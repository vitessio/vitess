package discovery

import (
	"errors"

	"github.com/spf13/pflag"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// DynamicDiscovery implements the Discovery interface for "discovering"
// Vitess components hardcoded in a json string. It inherits from JSONDiscovery.
//
// As an example, here's a minimal Dynamic object for a single Vitess cluster running locally
// (such as the one described in https://vitess.io/docs/get-started/local-docker):
//
// 		{
// 			"vtgates": [
// 				{
// 					"host": {
// 						"hostname": "127.0.0.1:15991"
// 					}
// 				}
// 			]
// 		}
//
// For more examples of various static file configurations, see the unit tests.
// Discovery Dynamic is very similar to static file discovery, but removes the need for a static file in memory.
// This allows for dynamic cluster discovery after initial vtadmin deploy without a topo.

type DynamicDiscovery struct {
	JSONDiscovery
}

// NewDynamic returns a DynamicDiscovery for the given cluster.
func NewDynamic(cluster *vtadminpb.Cluster, flags *pflag.FlagSet, args []string) (Discovery, error) {
	disco := &DynamicDiscovery{
		JSONDiscovery: JSONDiscovery{
			cluster: cluster,
		},
	}

	json := flags.String("discovery", "", "the json config object")
	if err := flags.Parse(args); err != nil {
		return nil, err
	}

	if json == nil || *json == "" {
		return nil, errors.New("must pass service discovery json config object")
	}

	bytes := []byte(*json)
	if err := disco.parseConfig(bytes); err != nil {
		return nil, err
	}

	return disco, nil
}
