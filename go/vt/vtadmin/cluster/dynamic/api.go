package dynamic

import (
	"net/http"

	"vitess.io/vitess/go/vt/vtadmin/cluster"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// API is the interface dynamic APIs must implement.
// It is implemented by vtadmin.API.
type API interface {
	vtadminpb.VTAdminServer
	WithCluster(c *cluster.Cluster, id string) API
	Handler() http.Handler
}
