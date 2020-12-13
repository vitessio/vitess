package cluster

import (
	"fmt"

	"vitess.io/vitess/go/vt/vtadmin/cluster/discovery"
	"vitess.io/vitess/go/vt/vtadmin/vtsql"
)

// Cluster is the self-contained unit of services required for vtadmin to talk
// to a vitess cluster. This consists of a discovery service, a database
// connection, and a vtctl client.
type Cluster struct {
	ID        string
	Name      string
	Discovery discovery.Discovery

	// (TODO|@amason): after merging #7128, this still requires some additional
	// work, so deferring this for now!
	// vtctl vtctldclient.VtctldClient
	DB vtsql.DB

	// These fields are kept to power debug endpoints.
	// (TODO|@amason): Figure out if these are needed or if there's a way to
	// push down to the credentials / vtsql.
	// vtgateCredentialsPath string
}

// New creates a new Cluster from a Config.
func New(cfg Config) (*Cluster, error) {
	cluster := &Cluster{
		ID:   cfg.ID,
		Name: cfg.Name,
	}

	discoargs := buildPFlagSlice(cfg.DiscoveryFlagsByImpl[cfg.DiscoveryImpl])

	disco, err := discovery.New(cfg.DiscoveryImpl, cluster.Name, discoargs)
	if err != nil {
		return nil, fmt.Errorf("error while creating discovery impl (%s): %w", cfg.DiscoveryImpl, err)
	}

	cluster.Discovery = disco

	vtsqlargs := buildPFlagSlice(cfg.VtSQLFlags)

	vtsqlCfg, err := vtsql.Parse(cluster.ID, cluster.Name, disco, vtsqlargs)
	if err != nil {
		return nil, fmt.Errorf("error while creating vtsql connection: %w", err)
	}

	cluster.DB = vtsql.New(cluster.Name, vtsqlCfg)

	return cluster, nil
}

func buildPFlagSlice(flags map[string]string) []string {
	args := make([]string, 0, len(flags))
	for k, v := range flags {
		// The k=v syntax is needed to account for negating boolean flags.
		args = append(args, "--"+k+"="+v)
	}

	return args
}
