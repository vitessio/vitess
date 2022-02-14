package discovery

import (
	"errors"
	"os"

	"github.com/spf13/pflag"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// StaticFileDiscovery implements the Discovery interface for "discovering"
// Vitess components hardcoded in a static JSON file. It inherits from JSONDiscovery.
//
// As an example, here's a minimal JSON file for a single Vitess cluster running locally
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
type StaticFileDiscovery struct {
	JSONDiscovery
}

// NewStaticFile returns a StaticFileDiscovery for the given cluster.
func NewStaticFile(cluster *vtadminpb.Cluster, flags *pflag.FlagSet, args []string) (Discovery, error) {
	disco := &StaticFileDiscovery{
		JSONDiscovery: JSONDiscovery{
			cluster: cluster,
		},
	}

	filePath := flags.String("path", "", "path to the service discovery JSON config file")
	if err := flags.Parse(args); err != nil {
		return nil, err
	}

	if filePath == nil || *filePath == "" {
		return nil, errors.New("must specify path to the service discovery JSON config file")
	}

	b, err := os.ReadFile(*filePath)
	if err != nil {
		return nil, err
	}

	if err := disco.parseConfig(b); err != nil {
		return nil, err
	}

	return disco, nil
}
