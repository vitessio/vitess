/*
Copyright 2021 The Vitess Authors.

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

/*
Package rbac provides role-based access control for vtadmin API endpoints.

Functionality is split between two distinct components: the authenticator and
the authorizer.

The authenticator is optional, and is responsible for extracting information
from a request (gRPC or HTTP) to produce an Actor, which is added to the context
by interceptors/middlewares and eventually checked by the authorizer.

The authorizer maintains a set of rules for each resource type, and, given a
request context, action, resource, and cluster, checks its ruleset to see if
the Actor in the context (set by some authenticator) has a rule allowing it to
perform that <action, resource, cluster> tuple.

The design of package rbac is governed by the following principles:

1. Authentication is pluggable. Authorization is configurable.

VTAdmin will not be specific about how exactly you authenticate users for your
setup. Instead, users can provide whatever implementation suits their needs that
conforms to the expected Authenticator interface, and vtadmin will use that when
setting up the interceptors/middlewares. Currently, authenticators may be
registered at runtime via the rbac.RegisterAuthenticator method, or may be set
as a Go plugin (built via `go build -buildmode=plugin`) by setting the
authenticator name as a path ending in ".so" in the rbac config.

2. Permissions are additive. There is no concept of a negative permission (or
revocation). To "revoke" a permission from a user or role, structure your rules
such that they are never granted that permission.

3. Authentication is done at the gRPC/HTTP ingress boundaries.

4. Authorization is done at the API boundary. Individual clusters do not perform
authorization checks, instead relying on the calling API method to perform that
check before calling into the cluster.

5. Being unauthorized for an <action, resource> for a cluster does not fail the
overall request. Instead, the action is simply not taken in that cluster, and is
still taken in other clusters for which the actor is authorized.
*/
package rbac

// Action is an enum representing the possible actions that can be taken. Not
// every resource supports every possible action.
type Action string

// Action definitions.
const (
	/* generic actions */

	CreateAction Action = "create"
	DeleteAction Action = "delete"
	GetAction    Action = "get"
	PingAction   Action = "ping"
	PutAction    Action = "put"
	ReloadAction Action = "reload"

	/* shard-specific actions */

	EmergencyFailoverShardAction   Action = "emergency_failover_shard"
	PlannedFailoverShardAction     Action = "planned_failover_shard"
	TabletExternallyPromotedAction Action = "tablet_externally_promoted" // NOTE: even though "tablet" is in the name, this actually operates on the tablet's shard.

	/* tablet-specific actions */

	ManageTabletReplicationAction        Action = "manage_tablet_replication" // Start/Stop Replication
	ManageTabletWritabilityAction        Action = "manage_tablet_writability" // SetRead{Only,Write}
	RefreshTabletReplicationSourceAction Action = "refresh_tablet_replication_source"
)

// Resource is an enum representing all resources managed by vtadmin.
type Resource string

// Resource definitions.
const (
	ClusterResource Resource = "Cluster"

	/* generic topo resources */

	CellInfoResource   Resource = "CellInfo"
	CellsAliasResource Resource = "CellsAlias"
	KeyspaceResource   Resource = "Keyspace"
	ShardResource      Resource = "Shard"
	TabletResource     Resource = "Tablet"
	VTGateResource     Resource = "VTGate"
	VtctldResource     Resource = "Vtctld"

	/* vschema resources */

	SrvVSchemaResource Resource = "SrvVSchema"
	VSchemaResource    Resource = "VSchema"

	/* misc resources */

	BackupResource                   Resource = "Backup"
	SchemaResource                   Resource = "Schema"
	ShardReplicationPositionResource Resource = "ShardReplicationPosition"
	WorkflowResource                 Resource = "Workflow"

	VTExplainResource Resource = "VTExplain"
)
