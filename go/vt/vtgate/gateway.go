/*
Copyright 2019 The Vitess Authors.
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

package vtgate

import (
	"flag"
	"time"

	"context"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This file contains the Gateway interface definition, and the
// implementations registry.
// A Gateway can take
// a query targeted to a keyspace/shard/tablet_type and send it off.

var (
	// GatewayImplementation allows you to choose which gateway to use for vtgate routing. Defaults to discoverygateway, other option is tabletgateway
	GatewayImplementation = flag.String("gateway_implementation", "tabletgateway", "Allowed values: discoverygateway (deprecated), tabletgateway (default)")
	initialTabletTimeout  = flag.Duration("gateway_initial_tablet_timeout", 30*time.Second, "At startup, the gateway will wait up to that duration to get one tablet per keyspace/shard/tablettype")
	// RetryCount is the number of times a query will be retried on error
	// Make this unexported after DiscoveryGateway is deprecated
	RetryCount = flag.Int("retry-count", 2, "retry count")
)

// A Gateway is the query processing module for each shard,
// which is used by ScatterConn.
type Gateway interface {
	// the query service that this Gateway wraps around
	queryservice.QueryService

	// WaitForTablets asks the gateway to wait for the provided
	// tablets types to be available. It the context is canceled
	// before the end, it should return ctx.Err().
	// The error returned will have specific effects:
	// - nil: keep going with startup.
	// - context.DeadlineExceeded: log a warning that we didn't get
	//   all tablets, and keep going with startup.
	// - any other error: log.Fatalf out.
	WaitForTablets(ctx context.Context, tabletTypesToWait []topodatapb.TabletType) error

	// RegisterStats registers exported stats for the gateway
	RegisterStats()

	// CacheStatus returns a list of TabletCacheStatus per shard / tablet type.
	CacheStatus() TabletCacheStatusList

	// TabletsCacheStatus returns a displayable version of the health check cache.
	TabletsCacheStatus() discovery.TabletsCacheStatusList

	// TabletByAlias returns a QueryService
	QueryServiceByAlias(alias *topodatapb.TabletAlias) (queryservice.QueryService, error)
}

// Creator is the factory method which can create the actual gateway object.
type Creator func(ctx context.Context, hc discovery.LegacyHealthCheck, serv srvtopo.Server, cell string, retryCount int) Gateway

var creators = make(map[string]Creator)

// RegisterGatewayCreator registers a Creator with given name.
func RegisterGatewayCreator(name string, gc Creator) {
	if _, ok := creators[name]; ok {
		log.Fatalf("Gateway %s already exists", name)
	}
	creators[name] = gc
}

// GatewayCreator returns the Creator specified by the gateway_implementation flag.
func GatewayCreator() Creator {
	gc, ok := creators[*GatewayImplementation]
	if !ok {
		log.Exitf("No gateway registered as %s", *GatewayImplementation)
	}
	return gc
}

// WaitForTablets is a helper method to wait for the provided tablets,
// up until the *initialTabletTimeout. It will log what it is doing.
// Note it has the same name as the Gateway's interface method, as it
// just calls it.
func WaitForTablets(gw Gateway, tabletTypesToWait []topodatapb.TabletType) error {
	log.Infof("Gateway waiting for serving tablets of types %v ...", tabletTypesToWait)
	ctx, cancel := context.WithTimeout(context.Background(), *initialTabletTimeout)
	defer cancel()

	err := gw.WaitForTablets(ctx, tabletTypesToWait)
	switch err {
	case nil:
		// Log so we know everything is fine.
		log.Infof("Waiting for tablets completed")
	case context.DeadlineExceeded:
		// In this scenario, we were able to reach the
		// topology service, but some tablets may not be
		// ready. We just warn and keep going.
		log.Warningf("Timeout waiting for all keyspaces / shards to have healthy tablets of types %v, may be in degraded mode", tabletTypesToWait)
		err = nil
	default:
		// Nothing to do here, the caller will log.Fatalf.
	}
	return err
}
