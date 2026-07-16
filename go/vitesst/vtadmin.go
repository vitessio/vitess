/*
Copyright 2026 The Vitess Authors.

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

package vitesst

import (
	"context"
	"fmt"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// vtadminHTTPPort is the vtadmin API port inside the container.
const vtadminHTTPPort = 14200

// vtadminRBAC is an allow-all RBAC policy for tests.
const vtadminRBAC = `rules:
  - resource: "*"
    actions: ["*"]
    subjects: ["*"]
    clusters: ["*"]
`

type (
	// VTAdmin is the runtime handle for the vtadmin API container, present
	// only when the cluster was created with WithVTAdmin.
	VTAdmin struct {
		component
	}

	vtadminOption []string
)

func (o vtadminOption) apply(opts *clusterOptions) {
	opts.vtadminEnabled = true
	opts.vtadminArgs = append(opts.vtadminArgs, o...)
}

// WithVTAdmin enables a vtadmin API container for the cluster, with optional
// extra arguments.
func WithVTAdmin(args ...string) ClusterOption {
	return vtadminOption(args)
}

type vtadminClusterIDOption string

func (o vtadminClusterIDOption) apply(opts *clusterOptions) {
	opts.vtadminClusterID = string(o)
}

// WithVTAdminClusterID overrides vtadmin's cluster identifier; the default is
// "local".
func WithVTAdminClusterID(id string) ClusterOption {
	return vtadminClusterIDOption(id)
}

// VTAdmin returns the cluster's vtadmin, or nil when it was not enabled.
func (c *Cluster) VTAdmin() *VTAdmin {
	return c.vtadmin
}

// startVTAdmin starts the vtadmin API container, discovering the cluster's
// vtctld and vtgate through their static network aliases.
func (c *Cluster) startVTAdmin(t testing.TB, ctx context.Context) error {
	vtadmin := &VTAdmin{
		component: component{
			name:     c.name("vtadmin"),
			httpPort: fmt.Sprintf("%d/tcp", vtadminHTTPPort),
			cluster:  c,
		},
	}

	discovery := fmt.Sprintf(`{
    "vtctlds": [
        {
            "host": {
                "fqdn": "%[1]s:%[2]d",
                "hostname": "%[1]s:%[3]d"
            }
        }
    ],
    "vtgates": [
        {
            "host": {
                "fqdn": "%[4]s:%[5]d",
                "hostname": "%[4]s:%[6]d"
            }
        }
    ]
}
`, c.vtctld.name, vtctldHTTPPort, vtctldGRPCPort, c.name("vtgate"), vtgateHTTPPort, vtgateGRPCPort)

	rbacPath := containerFilesDir + "/vtadmin_rbac.yaml"
	discoveryPath := containerFilesDir + "/vtadmin_discovery.json"
	filesOpt, err := withContainerFiles([]ContainerFile{
		{Content: []byte(vtadminRBAC), ContainerPath: rbacPath},
		{Content: []byte(discovery), ContainerPath: discoveryPath},
	})
	if err != nil {
		return fmt.Errorf("preparing vtadmin files: %w", err)
	}

	clusterID := c.opts.vtadminClusterID
	if clusterID == "" {
		clusterID = "local"
	}

	tabletURLTmpl := fmt.Sprintf("http://{{ .Tablet.Hostname }}:%d", tabletHTTPPort)
	args := []string{
		"vtadmin",
		"--addr", fmt.Sprintf(":%d", vtadminHTTPPort),
		"--http-tablet-url-tmpl", tabletURLTmpl,
		"--alsologtostderr",
		"--rbac",
		"--rbac-config", rbacPath,
		"--cluster", fmt.Sprintf(
			"id=%s,name=%s,discovery=staticfile,discovery-staticfile-path=%s,tablet-fqdn-tmpl=%s,schema-cache-default-expiration=1m",
			clusterID, clusterID, discoveryPath, tabletURLTmpl,
		),
	}
	args = append(args, c.opts.vtadminArgs...)

	ctr, err := testcontainers.Run(
		ctx, c.image,
		testcontainers.WithCmd(args...),
		testcontainers.WithExposedPorts(vtadmin.httpPort),
		network.WithNetwork([]string{vtadmin.name}, c.network),
		testcontainers.WithEnv(map[string]string{"VTTEST": "endtoend"}),
		filesOpt,
		testcontainers.WithLogConsumers(c.newFileLogConsumer(t, vtadmin.name)),
		testcontainers.WithWaitStrategyAndDeadline(
			defaultStartupTimeout,
			wait.ForListeningPort(vtadmin.httpPort).
				WithStartupTimeout(defaultStartupTimeout).
				WithPollInterval(defaultPollInterval),
		),
	)
	if err != nil {
		return fmt.Errorf("starting vtadmin: %w", err)
	}

	vtadmin.setContainer(ctr)
	c.vtadmin = vtadmin
	return nil
}
