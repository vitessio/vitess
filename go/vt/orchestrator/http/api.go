/*
   Copyright 2014 Outbrain Inc.

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

package http

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-martini/martini"
	"github.com/martini-contrib/auth"
	"github.com/martini-contrib/render"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/util"

	"vitess.io/vitess/go/vt/orchestrator/collection"
	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/discovery"
	"vitess.io/vitess/go/vt/orchestrator/inst"
	"vitess.io/vitess/go/vt/orchestrator/logic"
	"vitess.io/vitess/go/vt/orchestrator/metrics/query"
	"vitess.io/vitess/go/vt/orchestrator/process"
	orcraft "vitess.io/vitess/go/vt/orchestrator/raft"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/promotionrule"
)

// APIResponseCode is an OK/ERROR response code
type APIResponseCode int

const (
	ERROR APIResponseCode = iota
	OK
)

var registeredPaths = []string{}
var emptyInstanceKey inst.InstanceKey

func (apiResponseCode *APIResponseCode) MarshalJSON() ([]byte, error) {
	return json.Marshal(apiResponseCode.String())
}

func (apiResponseCode *APIResponseCode) String() string {
	switch *apiResponseCode {
	case ERROR:
		return "ERROR"
	case OK:
		return "OK"
	}
	return "unknown"
}

// HTTPStatus returns the respective HTTP status for this response
func (apiResponseCode *APIResponseCode) HTTPStatus() int {
	switch *apiResponseCode {
	case ERROR:
		return http.StatusInternalServerError
	case OK:
		return http.StatusOK
	}
	return http.StatusNotImplemented
}

// APIResponse is a response returned as JSON to various requests.
type APIResponse struct {
	Code    APIResponseCode
	Message string
	Details any
}

func Respond(r render.Render, apiResponse *APIResponse) {
	r.JSON(apiResponse.Code.HTTPStatus(), apiResponse)
}

type API struct {
	URLPrefix string
}

var HTTPapi = API{}
var discoveryMetrics = collection.CreateOrReturnCollection("DISCOVERY_METRICS")
var queryMetrics = collection.CreateOrReturnCollection("BACKEND_WRITES")
var writeBufferMetrics = collection.CreateOrReturnCollection("WRITE_BUFFER")

func (httpAPI *API) getInstanceKeyInternal(host string, port string, resolve bool) (inst.InstanceKey, error) {
	var instanceKey *inst.InstanceKey
	var err error
	if resolve {
		instanceKey, err = inst.NewResolveInstanceKeyStrings(host, port)
	} else {
		instanceKey, err = inst.NewRawInstanceKeyStrings(host, port)
	}
	if err != nil {
		return emptyInstanceKey, err
	}
	instanceKey, err = inst.FigureInstanceKey(instanceKey, nil)
	if err != nil {
		return emptyInstanceKey, err
	}
	if instanceKey == nil {
		return emptyInstanceKey, fmt.Errorf("Unexpected nil instanceKey in getInstanceKeyInternal(%+v, %+v, %+v)", host, port, resolve)
	}
	return *instanceKey, nil
}

func (httpAPI *API) getInstanceKey(host string, port string) (inst.InstanceKey, error) {
	return httpAPI.getInstanceKeyInternal(host, port, true)
}

func (httpAPI *API) getNoResolveInstanceKey(host string, port string) (inst.InstanceKey, error) {
	return httpAPI.getInstanceKeyInternal(host, port, false)
}

func getTag(params martini.Params, req *http.Request) (tag *inst.Tag, err error) {
	tagString := req.URL.Query().Get("tag")
	if tagString != "" {
		return inst.ParseTag(tagString)
	}
	return inst.NewTag(params["tagName"], params["tagValue"])
}

func (httpAPI *API) getBinlogCoordinates(logFile string, logPos string) (inst.BinlogCoordinates, error) {
	coordinates := inst.BinlogCoordinates{LogFile: logFile}
	var err error
	if coordinates.LogPos, err = strconv.ParseInt(logPos, 10, 0); err != nil {
		return coordinates, fmt.Errorf("Invalid logPos: %s", logPos)
	}

	return coordinates, err
}

// InstanceReplicas lists all replicas of given instance
func (httpAPI *API) InstanceReplicas(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	replicas, err := inst.ReadReplicaInstances(&instanceKey)

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", instanceKey)})
		return
	}
	r.JSON(http.StatusOK, replicas)
}

// Instance reads and returns an instance's details.
func (httpAPI *API) Instance(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, found, err := inst.ReadInstance(&instanceKey)
	if (!found) || (err != nil) {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", instanceKey)})
		return
	}
	r.JSON(http.StatusOK, instance)
}

// AsyncDiscover issues an asynchronous read on an instance. This is
// useful for bulk loads of a new set of instances and will not block
// if the instance is slow to respond or not reachable.
func (httpAPI *API) AsyncDiscover(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	go httpAPI.Discover(params, r, req, user)

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Asynchronous discovery initiated for Instance: %+v", instanceKey)})
}

// Discover issues a synchronous read on an instance
func (httpAPI *API) Discover(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.ReadTopologyInstance(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	if orcraft.IsRaftEnabled() {
		orcraft.PublishCommand("discover", instanceKey)
	} else {
		logic.DiscoverInstance(instanceKey, false /* forceDiscovery */)
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance discovered: %+v", instance.Key), Details: instance})
}

// Refresh synchronuously re-reads a topology instance
func (httpAPI *API) Refresh(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	_, err = inst.RefreshTopologyInstance(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance refreshed: %+v", instanceKey), Details: instanceKey})
}

// Forget removes an instance entry fro backend database
func (httpAPI *API) Forget(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getNoResolveInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("forget", instanceKey)
	} else {
		err = inst.ForgetInstance(&instanceKey)
	}
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance forgotten: %+v", instanceKey), Details: instanceKey})
}

// ForgetCluster forgets all instacnes of a cluster
func (httpAPI *API) ForgetCluster(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	clusterName, err := figureClusterName(getClusterHint(params))
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	if orcraft.IsRaftEnabled() {
		orcraft.PublishCommand("forget-cluster", clusterName)
	} else {
		inst.ForgetCluster(clusterName)
	}
	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Cluster forgotten: %+v", clusterName)})
}

// Resolve tries to resolve hostname and then checks to see if port is open on that host.
func (httpAPI *API) Resolve(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	if conn, err := net.Dial("tcp", instanceKey.DisplayString()); err == nil {
		conn.Close()
	} else {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: "Instance resolved", Details: instanceKey})
}

// BeginMaintenance begins maintenance mode for given instance
func (httpAPI *API) BeginMaintenance(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	key, err := inst.BeginBoundedMaintenance(&instanceKey, params["owner"], params["reason"], 0, true)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error(), Details: key})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Maintenance begun: %+v", instanceKey), Details: instanceKey})
}

// EndMaintenance terminates maintenance mode
func (httpAPI *API) EndMaintenance(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	maintenanceKey, err := strconv.ParseInt(params["maintenanceKey"], 10, 0)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	_, err = inst.EndMaintenance(maintenanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Maintenance ended: %+v", maintenanceKey), Details: maintenanceKey})
}

// EndMaintenanceByInstanceKey terminates maintenance mode for given instance
func (httpAPI *API) EndMaintenanceByInstanceKey(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	_, err = inst.EndMaintenanceByInstanceKey(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Maintenance ended: %+v", instanceKey), Details: instanceKey})
}

// EndMaintenanceByInstanceKey terminates maintenance mode for given instance
func (httpAPI *API) InMaintenance(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	inMaintenance, err := inst.InMaintenance(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	responseDetails := ""
	if inMaintenance {
		responseDetails = instanceKey.StringCode()
	}
	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("%+v", inMaintenance), Details: responseDetails})
}

// Maintenance provides list of instance under active maintenance
func (httpAPI *API) Maintenance(params martini.Params, r render.Render, req *http.Request) {
	maintenanceList, err := inst.ReadActiveMaintenance()

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, maintenanceList)
}

// BeginDowntime sets a downtime flag with default duration
func (httpAPI *API) BeginDowntime(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	var durationSeconds int
	if params["duration"] != "" {
		durationSeconds, err = util.SimpleTimeToSeconds(params["duration"])
		if durationSeconds < 0 {
			err = fmt.Errorf("Duration value must be non-negative. Given value: %d", durationSeconds)
		}
		if err != nil {
			Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
			return
		}
	}
	duration := time.Duration(durationSeconds) * time.Second
	downtime := inst.NewDowntime(&instanceKey, params["owner"], params["reason"], duration)
	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("begin-downtime", downtime)
	} else {
		err = inst.BeginDowntime(downtime)
	}

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error(), Details: instanceKey})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Downtime begun: %+v", instanceKey), Details: instanceKey})
}

// EndDowntime terminates downtime (removes downtime flag) for an instance
func (httpAPI *API) EndDowntime(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("end-downtime", instanceKey)
	} else {
		_, err = inst.EndDowntime(&instanceKey)
	}
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Downtime ended: %+v", instanceKey), Details: instanceKey})
}

// MoveUp attempts to move an instance up the topology
func (httpAPI *API) MoveUp(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.MoveUp(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v moved up", instanceKey), Details: instance})
}

// MoveUpReplicas attempts to move up all replicas of an instance
func (httpAPI *API) MoveUpReplicas(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	replicas, newPrimary, errs, err := inst.MoveUpReplicas(&instanceKey, req.URL.Query().Get("pattern"))
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Moved up %d replicas of %+v below %+v; %d errors: %+v", len(replicas), instanceKey, newPrimary.Key, len(errs), errs), Details: replicas})
}

// Repoint positiones a replica under another (or same) primary with exact same coordinates.
// Useful for binlog servers
func (httpAPI *API) Repoint(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := httpAPI.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.Repoint(&instanceKey, &belowKey, inst.GTIDHintNeutral)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v repointed below %+v", instanceKey, belowKey), Details: instance})
}

// MoveUpReplicas attempts to move up all replicas of an instance
func (httpAPI *API) RepointReplicas(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	replicas, _, err := inst.RepointReplicas(&instanceKey, req.URL.Query().Get("pattern"))
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Repointed %d replicas of %+v", len(replicas), instanceKey), Details: replicas})
}

// MakeCoPrimary attempts to make an instance co-primary with its own primary
func (httpAPI *API) MakeCoPrimary(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.MakeCoPrimary(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance made co-primary: %+v", instance.Key), Details: instance})
}

// ResetReplication makes a replica forget about its primary, effectively breaking the replication
func (httpAPI *API) ResetReplication(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.ResetReplicationOperation(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Replica reset on %+v", instance.Key), Details: instance})
}

// DetachReplicaPrimaryHost detaches a replica from its primary by setting an invalid
// (yet revertible) host name
func (httpAPI *API) DetachReplicaPrimaryHost(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.DetachReplicaPrimaryHost(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Replica detached: %+v", instance.Key), Details: instance})
}

func (httpAPI *API) NoOp(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	Respond(r, &APIResponse{Code: OK, Message: "API Deprecated"})
}

// EnableGTID attempts to enable GTID on a replica
func (httpAPI *API) EnableGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.EnableGTID(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Enabled GTID on %+v", instance.Key), Details: instance})
}

// DisableGTID attempts to disable GTID on a replica, and revert to binlog file:pos
func (httpAPI *API) DisableGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.DisableGTID(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Disabled GTID on %+v", instance.Key), Details: instance})
}

// LocateErrantGTID identifies the binlog positions for errant GTIDs on an instance
func (httpAPI *API) LocateErrantGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	errantBinlogs, err := inst.LocateErrantGTID(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	Respond(r, &APIResponse{Code: OK, Message: "located errant GTID", Details: errantBinlogs})
}

// ErrantGTIDResetPrimary removes errant transactions on a server by way of RESET MASTER
func (httpAPI *API) ErrantGTIDResetPrimary(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.ErrantGTIDResetPrimary(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Removed errant GTID on %+v and issued a RESET MASTER", instance.Key), Details: instance})
}

// ErrantGTIDInjectEmpty removes errant transactions by injecting and empty transaction on the cluster's primary
func (httpAPI *API) ErrantGTIDInjectEmpty(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, clusterPrimary, countInjectedTransactions, err := inst.ErrantGTIDInjectEmpty(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Have injected %+v transactions on cluster primary %+v", countInjectedTransactions, clusterPrimary.Key), Details: instance})
}

// MoveBelow attempts to move an instance below its supposed sibling
func (httpAPI *API) MoveBelow(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	siblingKey, err := httpAPI.getInstanceKey(params["siblingHost"], params["siblingPort"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.MoveBelow(&instanceKey, &siblingKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v moved below %+v", instanceKey, siblingKey), Details: instance})
}

// MoveBelowGTID attempts to move an instance below another, via GTID
func (httpAPI *API) MoveBelowGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := httpAPI.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.MoveBelowGTID(&instanceKey, &belowKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v moved below %+v via GTID", instanceKey, belowKey), Details: instance})
}

// MoveReplicasGTID attempts to move an instance below another, via GTID
func (httpAPI *API) MoveReplicasGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := httpAPI.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	movedReplicas, _, errs, err := inst.MoveReplicasGTID(&instanceKey, &belowKey, req.URL.Query().Get("pattern"))
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Moved %d replicas of %+v below %+v via GTID; %d errors: %+v", len(movedReplicas), instanceKey, belowKey, len(errs), errs), Details: belowKey})
}

// TakeSiblings
func (httpAPI *API) TakeSiblings(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, count, err := inst.TakeSiblings(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Took %d siblings of %+v", count, instanceKey), Details: instance})
}

// TakePrimary
func (httpAPI *API) TakePrimary(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.TakePrimary(&instanceKey, false)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("%+v took its primary", instanceKey), Details: instance})
}

// RelocateBelow attempts to move an instance below another, orchestrator choosing the best (potentially multi-step)
// relocation method
func (httpAPI *API) RelocateBelow(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := httpAPI.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	instance, err := inst.RelocateBelow(&instanceKey, &belowKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Instance %+v relocated below %+v", instanceKey, belowKey), Details: instance})
}

// Relocates attempts to smartly relocate replicas of a given instance below another
func (httpAPI *API) RelocateReplicas(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowKey, err := httpAPI.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	replicas, _, errs, err := inst.RelocateReplicas(&instanceKey, &belowKey, req.URL.Query().Get("pattern"))
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Relocated %d replicas of %+v below %+v; %d errors: %+v", len(replicas), instanceKey, belowKey, len(errs), errs), Details: replicas})
}

// RegroupReplicas attempts to pick a replica of a given instance and make it take its siblings, using any
// method possible (GTID, binlog servers)
func (httpAPI *API) RegroupReplicas(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	lostReplicas, equalReplicas, aheadReplicas, cannotReplicateReplicas, promotedReplica, err := inst.RegroupReplicas(&instanceKey, false, nil, nil)
	lostReplicas = append(lostReplicas, cannotReplicateReplicas...)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("promoted replica: %s, lost: %d, trivial: %d, pseudo-gtid: %d",
		promotedReplica.Key.DisplayString(), len(lostReplicas), len(equalReplicas), len(aheadReplicas)), Details: promotedReplica.Key})
}

// RegroupReplicasGTID attempts to pick a replica of a given instance and make it take its siblings, efficiently, using GTID
func (httpAPI *API) RegroupReplicasGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	lostReplicas, movedReplicas, cannotReplicateReplicas, promotedReplica, err := inst.RegroupReplicasGTID(&instanceKey, false, nil, nil, nil)
	lostReplicas = append(lostReplicas, cannotReplicateReplicas...)

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("promoted replica: %s, lost: %d, moved: %d",
		promotedReplica.Key.DisplayString(), len(lostReplicas), len(movedReplicas)), Details: promotedReplica.Key})
}

// RegroupReplicasBinlogServers attempts to pick a replica of a given instance and make it take its siblings, efficiently, using GTID
func (httpAPI *API) RegroupReplicasBinlogServers(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	_, promotedBinlogServer, err := inst.RegroupReplicasBinlogServers(&instanceKey, false)

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("promoted binlog server: %s",
		promotedBinlogServer.Key.DisplayString()), Details: promotedBinlogServer.Key})
}

// SkipQuery skips a single query on a failed replication instance
func (httpAPI *API) SkipQuery(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.SkipQuery(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Query skipped on %+v", instance.Key), Details: instance})
}

// StartReplication starts replication on given instance
func (httpAPI *API) StartReplication(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.StartReplication(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Replica started: %+v", instance.Key), Details: instance})
}

// RestartReplication stops & starts replication on given instance
func (httpAPI *API) RestartReplication(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.RestartReplication(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Replica restarted: %+v", instance.Key), Details: instance})
}

// StopReplication stops replication on given instance
func (httpAPI *API) StopReplication(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.StopReplication(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Replica stopped: %+v", instance.Key), Details: instance})
}

// StopReplicationNicely stops replication on given instance, such that sql thead is aligned with IO thread
func (httpAPI *API) StopReplicationNicely(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.StopReplicationNicely(&instanceKey, 0)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Replica stopped nicely: %+v", instance.Key), Details: instance})
}

// FlushBinaryLogs runs a single FLUSH BINARY LOGS
func (httpAPI *API) FlushBinaryLogs(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.FlushBinaryLogs(&instanceKey, 1)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Binary logs flushed on: %+v", instance.Key), Details: instance})
}

// PurgeBinaryLogs purges binary logs up to given binlog file
func (httpAPI *API) PurgeBinaryLogs(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	logFile := params["logFile"]
	if logFile == "" {
		Respond(r, &APIResponse{Code: ERROR, Message: "purge-binary-logs: expected log file name or 'latest'"})
		return
	}
	force := (req.URL.Query().Get("force") == "true") || (params["force"] == "true")
	var instance *inst.Instance
	if logFile == "latest" {
		instance, err = inst.PurgeBinaryLogsToLatest(&instanceKey, force)
	} else {
		instance, err = inst.PurgeBinaryLogsTo(&instanceKey, logFile, force)
	}
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Binary logs flushed on: %+v", instance.Key), Details: instance})
}

// RestartReplicationStatements receives a query to execute that requires a replication restart to apply.
// As an example, this may be `set global rpl_semi_sync_slave_enabled=1`. orchestrator will check
// replication status on given host and will wrap with appropriate stop/start statements, if need be.
func (httpAPI *API) RestartReplicationStatements(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	query := req.URL.Query().Get("q")
	statements, err := inst.GetReplicationRestartPreserveStatements(&instanceKey, query)

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("statements for: %+v", instanceKey), Details: statements})
}

// CanReplicateFrom attempts to move an instance below another via pseudo GTID matching of binlog entries
func (httpAPI *API) CanReplicateFrom(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, found, err := inst.ReadInstance(&instanceKey)
	if (!found) || (err != nil) {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", instanceKey)})
		return
	}
	belowKey, err := httpAPI.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowInstance, found, err := inst.ReadInstance(&belowKey)
	if (!found) || (err != nil) {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", belowKey)})
		return
	}

	canReplicate, err := instance.CanReplicateFrom(belowInstance)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("%t", canReplicate), Details: belowKey})
}

// CanReplicateFromGTID attempts to move an instance below another via GTID.
func (httpAPI *API) CanReplicateFromGTID(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, found, err := inst.ReadInstance(&instanceKey)
	if (!found) || (err != nil) {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", instanceKey)})
		return
	}
	belowKey, err := httpAPI.getInstanceKey(params["belowHost"], params["belowPort"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	belowInstance, found, err := inst.ReadInstance(&belowKey)
	if (!found) || (err != nil) {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", belowKey)})
		return
	}

	canReplicate, err := instance.CanReplicateFrom(belowInstance)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	if !canReplicate {
		Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("%t", canReplicate), Details: belowKey})
		return
	}
	err = inst.CheckMoveViaGTID(instance, belowInstance)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	canReplicate = (err == nil)

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("%t", canReplicate), Details: belowKey})
}

// SetReadOnly sets the global read_only variable
func (httpAPI *API) SetReadOnly(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.SetReadOnly(&instanceKey, true)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: "Server set as read-only", Details: instance})
}

// SetWriteable clear the global read_only variable
func (httpAPI *API) SetWriteable(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.SetReadOnly(&instanceKey, false)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: "Server set as writeable", Details: instance})
}

// KillQuery kills a query running on a server
func (httpAPI *API) KillQuery(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, _ := httpAPI.getInstanceKey(params["host"], params["port"])
	processID, err := strconv.ParseInt(params["process"], 10, 0)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, err := inst.KillQuery(&instanceKey, processID)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Query killed on : %+v", instance.Key), Details: instance})
}

// ASCIITopology returns an ascii graph of cluster's instances
func (httpAPI *API) asciiTopology(params martini.Params, r render.Render, req *http.Request, tabulated bool, printTags bool) {
	clusterName, err := figureClusterName(getClusterHint(params))
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	asciiOutput, err := inst.ASCIITopology(clusterName, "", tabulated, printTags)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Topology for cluster %s", clusterName), Details: asciiOutput})
}

// SnapshotTopologies triggers orchestrator to record a snapshot of host/primary for all known hosts.
func (httpAPI *API) SnapshotTopologies(params martini.Params, r render.Render, req *http.Request) {
	start := time.Now()
	if err := inst.SnapshotTopologies(); err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err), Details: fmt.Sprintf("Took %v", time.Since(start))})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: "Topology Snapshot completed", Details: fmt.Sprintf("Took %v", time.Since(start))})
}

// ASCIITopology returns an ascii graph of cluster's instances
func (httpAPI *API) ASCIITopology(params martini.Params, r render.Render, req *http.Request) {
	httpAPI.asciiTopology(params, r, req, false, false)
}

// ASCIITopologyTabulated returns an ascii graph of cluster's instances
func (httpAPI *API) ASCIITopologyTabulated(params martini.Params, r render.Render, req *http.Request) {
	httpAPI.asciiTopology(params, r, req, true, false)
}

// ASCIITopologyTags returns an ascii graph of cluster's instances and instance tags
func (httpAPI *API) ASCIITopologyTags(params martini.Params, r render.Render, req *http.Request) {
	httpAPI.asciiTopology(params, r, req, false, true)
}

// Cluster provides list of instances in given cluster
func (httpAPI *API) Cluster(params martini.Params, r render.Render, req *http.Request) {
	clusterName, err := figureClusterName(getClusterHint(params))
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	instances, err := inst.ReadClusterInstances(clusterName)

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, instances)
}

// ClusterByAlias provides list of instances in given cluster
func (httpAPI *API) ClusterByAlias(params martini.Params, r render.Render, req *http.Request) {
	params["clusterName"] = params["clusterAlias"]
	httpAPI.Cluster(params, r, req)
}

// ClusterByInstance provides list of instances in cluster an instance belongs to
func (httpAPI *API) ClusterByInstance(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	instance, found, err := inst.ReadInstance(&instanceKey)
	if (!found) || (err != nil) {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Cannot read instance: %+v", instanceKey)})
		return
	}

	params["clusterName"] = instance.ClusterName
	httpAPI.Cluster(params, r, req)
}

// ClusterInfo provides details of a given cluster
func (httpAPI *API) ClusterInfo(params martini.Params, r render.Render, req *http.Request) {
	clusterName, err := figureClusterName(getClusterHint(params))
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	clusterInfo, err := inst.ReadClusterInfo(clusterName)

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, clusterInfo)
}

// Cluster provides list of instances in given cluster
func (httpAPI *API) ClusterInfoByAlias(params martini.Params, r render.Render, req *http.Request) {
	params["clusterName"] = params["clusterAlias"]
	httpAPI.ClusterInfo(params, r, req)
}

// ClusterOSCReplicas returns heuristic list of OSC replicas
func (httpAPI *API) ClusterOSCReplicas(params martini.Params, r render.Render, req *http.Request) {
	clusterName, err := figureClusterName(getClusterHint(params))
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	instances, err := inst.GetClusterOSCReplicas(clusterName)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, instances)
}

// Clusters provides list of known clusters
func (httpAPI *API) Clusters(params martini.Params, r render.Render, req *http.Request) {
	clusterNames, err := inst.ReadClusters()

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, clusterNames)
}

// ClustersInfo provides list of known clusters, along with some added metadata per cluster
func (httpAPI *API) ClustersInfo(params martini.Params, r render.Render, req *http.Request) {
	clustersInfo, err := inst.ReadClustersInfo("")

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, clustersInfo)
}

// Tags lists existing tags for a given instance
func (httpAPI *API) Tags(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	tags, err := inst.ReadInstanceTags(&instanceKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	tagStrings := []string{}
	for _, tag := range tags {
		tagStrings = append(tagStrings, tag.String())
	}
	r.JSON(http.StatusOK, tagStrings)
}

// TagValue returns a given tag's value for a specific instance
func (httpAPI *API) TagValue(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	tag, err := getTag(params, req)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	tagExists, err := inst.ReadInstanceTag(&instanceKey, tag)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	if tagExists {
		r.JSON(http.StatusOK, tag.TagValue)
	} else {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("tag %s not found for %+v", tag.TagName, instanceKey)})
	}
}

// Tagged return instance keys tagged by "tag" query param
func (httpAPI *API) Tagged(params martini.Params, r render.Render, req *http.Request) {
	tagsString := req.URL.Query().Get("tag")
	instanceKeyMap, err := inst.GetInstanceKeysByTags(tagsString)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	r.JSON(http.StatusOK, instanceKeyMap.GetInstanceKeys())
}

// Tags adds a tag to a given instance
func (httpAPI *API) Tag(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	tag, err := getTag(params, req)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("put-instance-tag", inst.InstanceTag{Key: instanceKey, T: *tag})
	} else {
		err = inst.PutInstanceTag(&instanceKey, tag)
	}
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("%+v tagged with %s", instanceKey, tag.String()), Details: instanceKey})
}

// Untag removes a tag from an instance
func (httpAPI *API) Untag(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	tag, err := getTag(params, req)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	untagged, err := inst.Untag(&instanceKey, tag)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("%s removed from %+v instances", tag.TagName, len(*untagged)), Details: untagged.GetInstanceKeys()})
}

// UntagAll removes a tag from all matching instances
func (httpAPI *API) UntagAll(params martini.Params, r render.Render, req *http.Request) {
	tag, err := getTag(params, req)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	untagged, err := inst.Untag(nil, tag)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("%s removed from %+v instances", tag.TagName, len(*untagged)), Details: untagged.GetInstanceKeys()})
}

// Clusters provides list of known primaries
func (httpAPI *API) Primaries(params martini.Params, r render.Render, req *http.Request) {
	instances, err := inst.ReadWriteableClustersPrimaries()

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, instances)
}

// ClusterPrimary returns the writable primary of a given cluster
func (httpAPI *API) ClusterPrimary(params martini.Params, r render.Render, req *http.Request) {
	clusterName, err := figureClusterName(getClusterHint(params))
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	primaries, err := inst.ReadClusterPrimary(clusterName)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	if len(primaries) == 0 {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("No primaries found for %+v", clusterName)})
		return
	}

	r.JSON(http.StatusOK, primaries[0])
}

// Downtimed lists downtimed instances, potentially filtered by cluster
func (httpAPI *API) Downtimed(params martini.Params, r render.Render, req *http.Request) {
	clusterName, err := getClusterNameIfExists(params)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	instances, err := inst.ReadDowntimedInstances(clusterName)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, instances)
}

// AllInstances lists all known instances
func (httpAPI *API) AllInstances(params martini.Params, r render.Render, req *http.Request) {
	instances, err := inst.SearchInstances("")

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, instances)
}

// Search provides list of instances matching given search param via various criteria.
func (httpAPI *API) Search(params martini.Params, r render.Render, req *http.Request) {
	searchString := params["searchString"]
	if searchString == "" {
		searchString = req.URL.Query().Get("s")
	}
	instances, err := inst.SearchInstances(searchString)

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, instances)
}

// Problems provides list of instances with known problems
func (httpAPI *API) Problems(params martini.Params, r render.Render, req *http.Request) {
	clusterName := params["clusterName"]
	instances, err := inst.ReadProblemInstances(clusterName)

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, instances)
}

// Audit provides list of audit entries by given page number
func (httpAPI *API) Audit(params martini.Params, r render.Render, req *http.Request) {
	page, err := strconv.Atoi(params["page"])
	if err != nil || page < 0 {
		page = 0
	}
	var auditedInstanceKey *inst.InstanceKey
	if instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"]); err == nil {
		auditedInstanceKey = &instanceKey
	}

	audits, err := inst.ReadRecentAudit(auditedInstanceKey, page)

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, audits)
}

// HostnameResolveCache shows content of in-memory hostname cache
func (httpAPI *API) HostnameResolveCache(params martini.Params, r render.Render, req *http.Request) {
	content, err := inst.HostnameResolveCache()

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: "Cache retrieved", Details: content})
}

// ResetHostnameResolveCache clears in-memory hostname resovle cache
func (httpAPI *API) ResetHostnameResolveCache(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	err := inst.ResetHostnameResolveCache()

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: "Hostname cache cleared"})
}

// DeregisterHostnameUnresolve deregisters the unresolve name used previously
func (httpAPI *API) DeregisterHostnameUnresolve(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	var instanceKey *inst.InstanceKey
	if instKey, err := httpAPI.getInstanceKey(params["host"], params["port"]); err == nil {
		instanceKey = &instKey
	}

	var err error
	registration := inst.NewHostnameDeregistration(instanceKey)
	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("register-hostname-unresolve", registration)
	} else {
		err = inst.RegisterHostnameUnresolve(registration)
	}
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	Respond(r, &APIResponse{Code: OK, Message: "Hostname deregister unresolve completed", Details: instanceKey})
}

// RegisterHostnameUnresolve registers the unresolve name to use
func (httpAPI *API) RegisterHostnameUnresolve(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	var instanceKey *inst.InstanceKey
	if instKey, err := httpAPI.getInstanceKey(params["host"], params["port"]); err == nil {
		instanceKey = &instKey
	}

	hostname := params["virtualname"]
	var err error
	registration := inst.NewHostnameRegistration(instanceKey, hostname)
	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("register-hostname-unresolve", registration)
	} else {
		err = inst.RegisterHostnameUnresolve(registration)
	}
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	Respond(r, &APIResponse{Code: OK, Message: "Hostname register unresolve completed", Details: instanceKey})
}

// SubmitPoolInstances (re-)applies the list of hostnames for a given pool
func (httpAPI *API) SubmitPoolInstances(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	pool := params["pool"]
	instances := req.URL.Query().Get("instances")

	var err error
	submission := inst.NewPoolInstancesSubmission(pool, instances)
	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("submit-pool-instances", submission)
	} else {
		err = inst.ApplyPoolInstances(submission)
	}
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Applied %s pool instances", pool), Details: pool})
}

// SubmitPoolHostnames (re-)applies the list of hostnames for a given pool
func (httpAPI *API) ReadClusterPoolInstancesMap(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	clusterName := params["clusterName"]
	pool := params["pool"]

	poolInstancesMap, err := inst.ReadClusterPoolInstancesMap(clusterName, pool)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Read pool instances for cluster %s", clusterName), Details: poolInstancesMap})
}

// GetHeuristicClusterPoolInstances returns instances belonging to a cluster's pool
func (httpAPI *API) GetHeuristicClusterPoolInstances(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	clusterName, err := figureClusterName(getClusterHint(params))
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	pool := params["pool"]

	instances, err := inst.GetHeuristicClusterPoolInstances(clusterName, pool)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Heuristic pool instances for cluster %s", clusterName), Details: instances})
}

// GetHeuristicClusterPoolInstances returns instances belonging to a cluster's pool
func (httpAPI *API) GetHeuristicClusterPoolInstancesLag(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	clusterName := params["clusterName"]
	pool := params["pool"]

	lag, err := inst.GetHeuristicClusterPoolInstancesLag(clusterName, pool)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Heuristic pool lag for cluster %s", clusterName), Details: lag})
}

// ReloadClusterAlias clears in-memory hostname resovle cache
func (httpAPI *API) ReloadClusterAlias(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	Respond(r, &APIResponse{Code: ERROR, Message: "This API call has been retired"})
}

// BulkPromotionRules returns a list of the known promotion rules for each instance
func (httpAPI *API) BulkPromotionRules(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	promotionRules, err := inst.BulkReadCandidateDatabaseInstance()
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, promotionRules)
}

// BulkInstances returns a list of all known instances
func (httpAPI *API) BulkInstances(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	instances, err := inst.BulkReadInstance()
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, instances)
}

// DiscoveryMetricsRaw will return the last X seconds worth of discovery information in time based order as a JSON array
func (httpAPI *API) DiscoveryMetricsRaw(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	seconds, err := strconv.Atoi(params["seconds"])
	if err != nil || seconds <= 0 {
		Respond(r, &APIResponse{Code: ERROR, Message: "Invalid value provided for seconds"})
		return
	}

	refTime := time.Now().Add(-time.Duration(seconds) * time.Second)
	json, err := discovery.JSONSince(discoveryMetrics, refTime)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unable to determine start time. Perhaps seconds value is wrong?"})
		return
	}
	log.Infof("DiscoveryMetricsRaw data: retrieved %d entries from discovery.MC", len(json))

	r.JSON(http.StatusOK, json)
}

// DiscoveryMetricsAggregated will return a single set of aggregated metrics for raw values collected since the
// specified time.
func (httpAPI *API) DiscoveryMetricsAggregated(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	seconds, _ := strconv.Atoi(params["seconds"])

	refTime := time.Now().Add(-time.Duration(seconds) * time.Second)
	aggregated, err := discovery.AggregatedSince(discoveryMetrics, refTime)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unable to generate aggregated discovery metrics"})
		return
	}
	// log.Debugf("DiscoveryMetricsAggregated data: %+v", aggregated)
	r.JSON(http.StatusOK, aggregated)
}

// DiscoveryQueueMetricsRaw returns the raw queue metrics (active and
// queued values), data taken secondly for the last N seconds.
func (httpAPI *API) DiscoveryQueueMetricsRaw(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	seconds, err := strconv.Atoi(params["seconds"])
	log.Infof("DiscoveryQueueMetricsRaw: seconds: %d", seconds)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unable to generate discovery queue  aggregated metrics"})
		return
	}

	queue := discovery.CreateOrReturnQueue("DEFAULT")
	metrics := queue.DiscoveryQueueMetrics(seconds)
	log.Infof("DiscoveryQueueMetricsRaw data: %+v", metrics)

	r.JSON(http.StatusOK, metrics)
}

// DiscoveryQueueMetricsAggregated returns a single value showing the metrics of the discovery queue over the last N seconds.
// This is expected to be called every 60 seconds (?) and the config setting of the retention period is currently hard-coded.
// See go/discovery/ for more information.
func (httpAPI *API) DiscoveryQueueMetricsAggregated(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	seconds, err := strconv.Atoi(params["seconds"])
	log.Infof("DiscoveryQueueMetricsAggregated: seconds: %d", seconds)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unable to generate discovery queue aggregated metrics"})
		return
	}

	queue := discovery.CreateOrReturnQueue("DEFAULT")
	aggregated := queue.AggregatedDiscoveryQueueMetrics(seconds)
	log.Infof("DiscoveryQueueMetricsAggregated data: %+v", aggregated)

	r.JSON(http.StatusOK, aggregated)
}

// BackendQueryMetricsRaw returns the raw backend query metrics
func (httpAPI *API) BackendQueryMetricsRaw(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	seconds, err := strconv.Atoi(params["seconds"])
	log.Infof("BackendQueryMetricsRaw: seconds: %d", seconds)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unable to generate raw backend query metrics"})
		return
	}

	refTime := time.Now().Add(-time.Duration(seconds) * time.Second)
	m, err := queryMetrics.Since(refTime)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unable to return backend query metrics"})
		return
	}

	log.Infof("BackendQueryMetricsRaw data: %+v", m)

	r.JSON(http.StatusOK, m)
}

func (httpAPI *API) BackendQueryMetricsAggregated(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	seconds, err := strconv.Atoi(params["seconds"])
	log.Infof("BackendQueryMetricsAggregated: seconds: %d", seconds)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unable to aggregated generate backend query metrics"})
		return
	}

	refTime := time.Now().Add(-time.Duration(seconds) * time.Second)
	aggregated := query.AggregatedSince(queryMetrics, refTime)
	log.Infof("BackendQueryMetricsAggregated data: %+v", aggregated)

	r.JSON(http.StatusOK, aggregated)
}

// WriteBufferMetricsRaw returns the raw instance write buffer metrics
func (httpAPI *API) WriteBufferMetricsRaw(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	seconds, err := strconv.Atoi(params["seconds"])
	log.Infof("WriteBufferMetricsRaw: seconds: %d", seconds)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unable to generate raw instance write buffer metrics"})
		return
	}

	refTime := time.Now().Add(-time.Duration(seconds) * time.Second)
	m, err := writeBufferMetrics.Since(refTime)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unable to return instance write buffermetrics"})
		return
	}

	log.Infof("WriteBufferMetricsRaw data: %+v", m)

	r.JSON(http.StatusOK, m)
}

// WriteBufferMetricsAggregated provides aggregate metrics of instance write buffer metrics
func (httpAPI *API) WriteBufferMetricsAggregated(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	seconds, err := strconv.Atoi(params["seconds"])
	log.Infof("WriteBufferMetricsAggregated: seconds: %d", seconds)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unable to aggregated instance write buffer metrics"})
		return
	}

	refTime := time.Now().Add(-time.Duration(seconds) * time.Second)
	aggregated := inst.AggregatedSince(writeBufferMetrics, refTime)
	log.Infof("WriteBufferMetricsAggregated data: %+v", aggregated)

	r.JSON(http.StatusOK, aggregated)
}

// Headers is a self-test call which returns HTTP headers
func (httpAPI *API) Headers(params martini.Params, r render.Render, req *http.Request) {
	r.JSON(http.StatusOK, req.Header)
}

// Health performs a self test
func (httpAPI *API) Health(params martini.Params, r render.Render, req *http.Request) {
	health, err := process.HealthTest()
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Application node is unhealthy %+v", err), Details: health})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: "Application node is healthy", Details: health})

}

// LBCheck returns a constant respnse, and this can be used by load balancers that expect a given string.
func (httpAPI *API) LBCheck(params martini.Params, r render.Render, req *http.Request) {
	r.JSON(http.StatusOK, "OK")
}

// LBCheck returns a constant respnse, and this can be used by load balancers that expect a given string.
func (httpAPI *API) LeaderCheck(params martini.Params, r render.Render, req *http.Request) {
	respondStatus, err := strconv.Atoi(params["errorStatusCode"])
	if err != nil || respondStatus < 0 {
		respondStatus = http.StatusNotFound
	}

	if logic.IsLeader() {
		r.JSON(http.StatusOK, "OK")
	} else {
		r.JSON(respondStatus, "Not leader")
	}
}

// A configurable endpoint that can be for regular status checks or whatever.  While similar to
// Health() this returns 500 on failure.  This will prevent issues for those that have come to
// expect a 200
// It might be a good idea to deprecate the current Health() behavior and roll this in at some
// point
func (httpAPI *API) StatusCheck(params martini.Params, r render.Render, req *http.Request) {
	health, err := process.HealthTest()
	if err != nil {
		r.JSON(500, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Application node is unhealthy %+v", err), Details: health})
		return
	}
	Respond(r, &APIResponse{Code: OK, Message: "Application node is healthy", Details: health})
}

// GrabElection forcibly grabs leadership. Use with care!!
func (httpAPI *API) GrabElection(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	err := process.GrabElection()
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Unable to grab election: %+v", err)})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: "Node elected as leader"})
}

// Reelect causes re-elections for an active node
func (httpAPI *API) Reelect(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	err := process.Reelect()
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Unable to re-elect: %+v", err)})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: "Set re-elections"})
}

// ReloadConfiguration reloads confiug settings (not all of which will apply after change)
func (httpAPI *API) ReloadConfiguration(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	extraConfigFile := req.URL.Query().Get("config")
	config.Reload(extraConfigFile)
	inst.AuditOperation("reload-configuration", nil, "Triggered via API")

	Respond(r, &APIResponse{Code: OK, Message: "Config reloaded", Details: extraConfigFile})
}

// ReplicationAnalysis retuens list of issues
func (httpAPI *API) replicationAnalysis(clusterName string, instanceKey *inst.InstanceKey, params martini.Params, r render.Render, req *http.Request) {
	analysis, err := inst.GetReplicationAnalysis(clusterName, &inst.ReplicationAnalysisHints{IncludeDowntimed: true})
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Cannot get analysis: %+v", err)})
		return
	}
	// Possibly filter single instance
	if instanceKey != nil {
		filtered := analysis[:0]
		for _, analysisEntry := range analysis {
			if instanceKey.Equals(&analysisEntry.AnalyzedInstanceKey) {
				filtered = append(filtered, analysisEntry)
			}
		}
		analysis = filtered
	}

	Respond(r, &APIResponse{Code: OK, Message: "Analysis", Details: analysis})
}

// ReplicationAnalysis retuens list of issues
func (httpAPI *API) ReplicationAnalysis(params martini.Params, r render.Render, req *http.Request) {
	httpAPI.replicationAnalysis("", nil, params, r, req)
}

// ReplicationAnalysis retuens list of issues
func (httpAPI *API) ReplicationAnalysisForCluster(params martini.Params, r render.Render, req *http.Request) {
	clusterName := params["clusterName"]
	if clusterName == "" {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Cannot get cluster name: %+v", params["clusterName"])})
		return
	}
	httpAPI.replicationAnalysis(clusterName, nil, params, r, req)
}

// ReplicationAnalysis retuens list of issues
func (httpAPI *API) ReplicationAnalysisForKey(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Cannot get analysis: %+v", err)})
		return
	}
	if !instanceKey.IsValid() {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("Cannot get analysis: invalid key %+v", instanceKey)})
		return
	}
	httpAPI.replicationAnalysis("", &instanceKey, params, r, req)
}

// RecoverLite attempts recovery on a given instance, without executing external processes
func (httpAPI *API) RecoverLite(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	params["skipProcesses"] = "true"
	httpAPI.Recover(params, r, req, user)
}

// Recover attempts recovery on a given instance
func (httpAPI *API) Recover(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	var candidateKey *inst.InstanceKey
	if key, err := httpAPI.getInstanceKey(params["candidateHost"], params["candidatePort"]); err == nil {
		candidateKey = &key
	}

	skipProcesses := (req.URL.Query().Get("skipProcesses") == "true") || (params["skipProcesses"] == "true")
	recoveryAttempted, promotedInstanceKey, err := logic.CheckAndRecover(&instanceKey, candidateKey, skipProcesses)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error(), Details: instanceKey})
		return
	}
	if !recoveryAttempted {
		Respond(r, &APIResponse{Code: ERROR, Message: "Recovery not attempted", Details: instanceKey})
		return
	}
	if promotedInstanceKey == nil {
		Respond(r, &APIResponse{Code: ERROR, Message: "Recovery attempted but no instance promoted", Details: instanceKey})
		return
	}
	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Recovery executed on %+v", instanceKey), Details: *promotedInstanceKey})
}

// GracefulPrimaryTakeover gracefully fails over a primary onto its single replica.
func (httpAPI *API) gracefulPrimaryTakeover(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	clusterName, err := figureClusterName(getClusterHint(params))
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	designatedKey, _ := httpAPI.getInstanceKey(params["designatedHost"], params["designatedPort"])
	// designatedKey may be empty/invalid
	topologyRecovery, err := logic.GracefulPrimaryTakeover(clusterName, &designatedKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error(), Details: topologyRecovery})
		return
	}
	if topologyRecovery == nil || topologyRecovery.SuccessorKey == nil {
		Respond(r, &APIResponse{Code: ERROR, Message: "graceful-primary-takeover: no successor promoted", Details: topologyRecovery})
		return
	}
	Respond(r, &APIResponse{Code: OK, Message: "graceful-primary-takeover: successor promoted", Details: topologyRecovery})
}

// GracefulPrimaryTakeover gracefully fails over a primary, either:
// - onto its single replica, or
// - onto a replica indicated by the user
func (httpAPI *API) GracefulPrimaryTakeover(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	httpAPI.gracefulPrimaryTakeover(params, r, req, user)
}

// GracefulPrimaryTakeoverAuto gracefully fails over a primary onto a replica of orchestrator's choosing
func (httpAPI *API) GracefulPrimaryTakeoverAuto(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	httpAPI.gracefulPrimaryTakeover(params, r, req, user)
}

// ForcePrimaryFailover fails over a primary (even if there's no particular problem with the primary)
func (httpAPI *API) ForcePrimaryFailover(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	clusterName, err := figureClusterName(getClusterHint(params))
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	topologyRecovery, err := logic.ForcePrimaryFailover(clusterName)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	if topologyRecovery.SuccessorKey != nil {
		Respond(r, &APIResponse{Code: OK, Message: "Primary failed over", Details: topologyRecovery})
	} else {
		Respond(r, &APIResponse{Code: ERROR, Message: "Primary not failed over", Details: topologyRecovery})
	}
}

// ForcePrimaryTakeover fails over a primary (even if there's no particular problem with the primary)
func (httpAPI *API) ForcePrimaryTakeover(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	clusterName, err := figureClusterName(getClusterHint(params))
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	designatedKey, _ := httpAPI.getInstanceKey(params["designatedHost"], params["designatedPort"])
	designatedInstance, _, err := inst.ReadInstance(&designatedKey)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	if designatedInstance == nil {
		Respond(r, &APIResponse{Code: ERROR, Message: "Instance not found"})
		return
	}

	topologyRecovery, err := logic.ForcePrimaryTakeover(clusterName, designatedInstance)
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	if topologyRecovery.SuccessorKey != nil {
		Respond(r, &APIResponse{Code: OK, Message: "Primary failed over", Details: topologyRecovery})
	} else {
		Respond(r, &APIResponse{Code: ERROR, Message: "Primary not failed over", Details: topologyRecovery})
	}
}

// Registers promotion preference for given instance
func (httpAPI *API) RegisterCandidate(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}
	promotionRule, err := promotionrule.Parse(params["promotionRule"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	candidate := inst.NewCandidateDatabaseInstance(&instanceKey, promotionRule).WithCurrentTime()

	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("register-candidate", candidate)
	} else {
		err = inst.RegisterCandidateInstance(candidate)
	}

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: "Registered candidate", Details: instanceKey})
}

// AutomatedRecoveryFilters retuens list of clusters which are configured with automated recovery
func (httpAPI *API) AutomatedRecoveryFilters(params martini.Params, r render.Render, req *http.Request) {
	automatedRecoveryMap := make(map[string]any)
	automatedRecoveryMap["RecoverPrimaryClusterFilters"] = config.Config.RecoverPrimaryClusterFilters
	automatedRecoveryMap["RecoverIntermediatePrimaryClusterFilters"] = config.Config.RecoverIntermediatePrimaryClusterFilters
	automatedRecoveryMap["RecoveryIgnoreHostnameFilters"] = config.Config.RecoveryIgnoreHostnameFilters

	Respond(r, &APIResponse{Code: OK, Message: "Automated recovery configuration details", Details: automatedRecoveryMap})
}

// AuditFailureDetection provides list of topology_failure_detection entries
func (httpAPI *API) AuditFailureDetection(params martini.Params, r render.Render, req *http.Request) {

	var audits []*logic.TopologyRecovery
	var err error

	if detectionID, derr := strconv.ParseInt(params["id"], 10, 0); derr == nil && detectionID > 0 {
		audits, err = logic.ReadFailureDetection(detectionID)
	} else {
		page, derr := strconv.Atoi(params["page"])
		if derr != nil || page < 0 {
			page = 0
		}
		audits, err = logic.ReadRecentFailureDetections(params["clusterName"], page)
	}

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, audits)
}

// AuditRecoverySteps returns audited steps of a given recovery
func (httpAPI *API) AuditRecoverySteps(params martini.Params, r render.Render, req *http.Request) {
	recoveryUID := params["uid"]
	audits, err := logic.ReadTopologyRecoverySteps(recoveryUID)

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, audits)
}

// ReadReplicationAnalysisChangelog lists instances and their analysis changelog
func (httpAPI *API) ReadReplicationAnalysisChangelog(params martini.Params, r render.Render, req *http.Request) {
	changelogs, err := inst.ReadReplicationAnalysisChangelog()

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, changelogs)
}

// AuditRecovery provides list of topology-recovery entries
func (httpAPI *API) AuditRecovery(params martini.Params, r render.Render, req *http.Request) {
	var audits []*logic.TopologyRecovery
	var err error

	if recoveryUID := params["uid"]; recoveryUID != "" {
		audits, err = logic.ReadRecoveryByUID(recoveryUID)
	} else if recoveryID, derr := strconv.ParseInt(params["id"], 10, 0); derr == nil && recoveryID > 0 {
		audits, err = logic.ReadRecovery(recoveryID)
	} else {
		page, derr := strconv.Atoi(params["page"])
		if derr != nil || page < 0 {
			page = 0
		}
		unacknowledgedOnly := (req.URL.Query().Get("unacknowledged") == "true")

		audits, err = logic.ReadRecentRecoveries(params["clusterName"], unacknowledgedOnly, page)
	}

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, audits)
}

// ActiveClusterRecovery returns recoveries in-progress for a given cluster
func (httpAPI *API) ActiveClusterRecovery(params martini.Params, r render.Render, req *http.Request) {
	recoveries, err := logic.ReadActiveClusterRecovery(params["clusterName"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, recoveries)
}

// RecentlyActiveClusterRecovery returns recoveries in-progress for a given cluster
func (httpAPI *API) RecentlyActiveClusterRecovery(params martini.Params, r render.Render, req *http.Request) {
	recoveries, err := logic.ReadRecentlyActiveClusterRecovery(params["clusterName"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, recoveries)
}

// RecentlyActiveClusterRecovery returns recoveries in-progress for a given cluster
func (httpAPI *API) RecentlyActiveInstanceRecovery(params martini.Params, r render.Render, req *http.Request) {
	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	recoveries, err := logic.ReadRecentlyActiveInstanceRecovery(&instanceKey)

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, recoveries)
}

// ClusterInfo provides details of a given cluster
func (httpAPI *API) AcknowledgeClusterRecoveries(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	var clusterName string
	var err error
	if params["clusterAlias"] != "" {
		clusterName = params["clusterAlias"]
	} else {
		clusterName, err = figureClusterName(getClusterHint(params))
	}

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	comment := strings.TrimSpace(req.URL.Query().Get("comment"))
	if comment == "" {
		Respond(r, &APIResponse{Code: ERROR, Message: "No acknowledge comment given"})
		return
	}
	userID := getUserID(req, user)
	if userID == "" {
		userID = inst.GetMaintenanceOwner()
	}
	if orcraft.IsRaftEnabled() {
		ack := logic.NewRecoveryAcknowledgement(userID, comment)
		ack.ClusterName = clusterName
		_, err = orcraft.PublishCommand("ack-recovery", ack)
	} else {
		_, err = logic.AcknowledgeClusterRecoveries(clusterName, userID, comment)
	}
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: "Acknowledged cluster recoveries", Details: clusterName})
}

// ClusterInfo provides details of a given cluster
func (httpAPI *API) AcknowledgeInstanceRecoveries(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	instanceKey, err := httpAPI.getInstanceKey(params["host"], params["port"])
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
		return
	}

	comment := strings.TrimSpace(req.URL.Query().Get("comment"))
	if comment == "" {
		Respond(r, &APIResponse{Code: ERROR, Message: "No acknowledge comment given"})
		return
	}
	userID := getUserID(req, user)
	if userID == "" {
		userID = inst.GetMaintenanceOwner()
	}
	if orcraft.IsRaftEnabled() {
		ack := logic.NewRecoveryAcknowledgement(userID, comment)
		ack.Key = instanceKey
		_, err = orcraft.PublishCommand("ack-recovery", ack)
	} else {
		_, err = logic.AcknowledgeInstanceRecoveries(&instanceKey, userID, comment)
	}
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: "Acknowledged instance recoveries", Details: instanceKey})
}

// ClusterInfo provides details of a given cluster
func (httpAPI *API) AcknowledgeRecovery(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}
	var err error
	var recoveryID int64
	var idParam string

	// Ack either via id or uid
	recoveryUID := params["uid"]
	if recoveryUID == "" {
		idParam = params["recoveryId"]
		recoveryID, err = strconv.ParseInt(idParam, 10, 0)
		if err != nil {
			Respond(r, &APIResponse{Code: ERROR, Message: err.Error()})
			return
		}
	} else {
		idParam = recoveryUID
	}
	comment := strings.TrimSpace(req.URL.Query().Get("comment"))
	if comment == "" {
		Respond(r, &APIResponse{Code: ERROR, Message: "No acknowledge comment given"})
		return
	}
	userID := getUserID(req, user)
	if userID == "" {
		userID = inst.GetMaintenanceOwner()
	}
	if orcraft.IsRaftEnabled() {
		ack := logic.NewRecoveryAcknowledgement(userID, comment)
		ack.ID = recoveryID
		ack.UID = recoveryUID
		_, err = orcraft.PublishCommand("ack-recovery", ack)
	} else {
		if recoveryUID != "" {
			_, err = logic.AcknowledgeRecoveryByUID(recoveryUID, userID, comment)
		} else {
			_, err = logic.AcknowledgeRecovery(recoveryID, userID, comment)
		}
	}

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: "Acknowledged recovery", Details: idParam})
}

// ClusterInfo provides details of a given cluster
func (httpAPI *API) AcknowledgeAllRecoveries(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	comment := strings.TrimSpace(req.URL.Query().Get("comment"))
	if comment == "" {
		Respond(r, &APIResponse{Code: ERROR, Message: "No acknowledge comment given"})
		return
	}
	userID := getUserID(req, user)
	if userID == "" {
		userID = inst.GetMaintenanceOwner()
	}
	var err error
	if orcraft.IsRaftEnabled() {
		ack := logic.NewRecoveryAcknowledgement(userID, comment)
		ack.AllRecoveries = true
		_, err = orcraft.PublishCommand("ack-recovery", ack)
	} else {
		_, err = logic.AcknowledgeAllRecoveries(userID, comment)
	}
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: "Acknowledged all recoveries", Details: comment})
}

// BlockedRecoveries reads list of currently blocked recoveries, optionally filtered by cluster name
func (httpAPI *API) BlockedRecoveries(params martini.Params, r render.Render, req *http.Request) {
	blockedRecoveries, err := logic.ReadBlockedRecoveries(params["clusterName"])

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	r.JSON(http.StatusOK, blockedRecoveries)
}

// DisableGlobalRecoveries globally disables recoveries
func (httpAPI *API) DisableGlobalRecoveries(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	var err error
	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("disable-global-recoveries", 0)
	} else {
		err = logic.DisableRecovery()
	}

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: "Globally disabled recoveries", Details: "disabled"})
}

// EnableGlobalRecoveries globally enables recoveries
func (httpAPI *API) EnableGlobalRecoveries(params martini.Params, r render.Render, req *http.Request, user auth.User) {
	if !isAuthorizedForAction(req, user) {
		Respond(r, &APIResponse{Code: ERROR, Message: "Unauthorized"})
		return
	}

	var err error
	if orcraft.IsRaftEnabled() {
		_, err = orcraft.PublishCommand("enable-global-recoveries", 0)
	} else {
		err = logic.EnableRecovery()
	}
	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}

	Respond(r, &APIResponse{Code: OK, Message: "Globally enabled recoveries", Details: "enabled"})
}

// CheckGlobalRecoveries checks whether
func (httpAPI *API) CheckGlobalRecoveries(params martini.Params, r render.Render, req *http.Request) {
	isDisabled, err := logic.IsRecoveryDisabled()

	if err != nil {
		Respond(r, &APIResponse{Code: ERROR, Message: fmt.Sprintf("%+v", err)})
		return
	}
	details := "enabled"
	if isDisabled {
		details = "disabled"
	}
	Respond(r, &APIResponse{Code: OK, Message: fmt.Sprintf("Global recoveries %+v", details), Details: details})
}

func (httpAPI *API) registerSingleAPIRequest(m *martini.ClassicMartini, path string, handler martini.Handler, allowProxy bool) {
	registeredPaths = append(registeredPaths, path)
	fullPath := fmt.Sprintf("%s/api/%s", httpAPI.URLPrefix, path)

	m.Get(fullPath, handler)
}

func (httpAPI *API) registerAPIRequestInternal(m *martini.ClassicMartini, path string, handler martini.Handler, allowProxy bool) {
	httpAPI.registerSingleAPIRequest(m, path, handler, allowProxy)
}

func (httpAPI *API) registerAPIRequest(m *martini.ClassicMartini, path string, handler martini.Handler) {
	httpAPI.registerAPIRequestInternal(m, path, handler, true)
}

func (httpAPI *API) registerAPIRequestNoProxy(m *martini.ClassicMartini, path string, handler martini.Handler) {
	httpAPI.registerAPIRequestInternal(m, path, handler, false)
}

// RegisterRequests makes for the de-facto list of known API calls
func (httpAPI *API) RegisterRequests(m *martini.ClassicMartini) {
	// Smart relocation:
	httpAPI.registerAPIRequest(m, "relocate/:host/:port/:belowHost/:belowPort", httpAPI.RelocateBelow)
	httpAPI.registerAPIRequest(m, "relocate-below/:host/:port/:belowHost/:belowPort", httpAPI.RelocateBelow)
	httpAPI.registerAPIRequest(m, "relocate-replicas/:host/:port/:belowHost/:belowPort", httpAPI.RelocateReplicas)
	httpAPI.registerAPIRequest(m, "regroup-replicas/:host/:port", httpAPI.RegroupReplicas)

	// Classic file:pos relocation:
	httpAPI.registerAPIRequest(m, "move-up/:host/:port", httpAPI.MoveUp)
	httpAPI.registerAPIRequest(m, "move-up-replicas/:host/:port", httpAPI.MoveUpReplicas)
	httpAPI.registerAPIRequest(m, "move-below/:host/:port/:siblingHost/:siblingPort", httpAPI.MoveBelow)
	httpAPI.registerAPIRequest(m, "repoint/:host/:port/:belowHost/:belowPort", httpAPI.Repoint)
	httpAPI.registerAPIRequest(m, "repoint-replicas/:host/:port", httpAPI.RepointReplicas)
	httpAPI.registerAPIRequest(m, "make-co-primary/:host/:port", httpAPI.MakeCoPrimary)
	httpAPI.registerAPIRequest(m, "take-siblings/:host/:port", httpAPI.TakeSiblings)
	httpAPI.registerAPIRequest(m, "take-primary/:host/:port", httpAPI.TakePrimary)

	// Binlog server relocation:
	httpAPI.registerAPIRequest(m, "regroup-replicas-bls/:host/:port", httpAPI.RegroupReplicasBinlogServers)

	// GTID relocation:
	httpAPI.registerAPIRequest(m, "move-below-gtid/:host/:port/:belowHost/:belowPort", httpAPI.MoveBelowGTID)
	httpAPI.registerAPIRequest(m, "move-replicas-gtid/:host/:port/:belowHost/:belowPort", httpAPI.MoveReplicasGTID)
	httpAPI.registerAPIRequest(m, "regroup-replicas-gtid/:host/:port", httpAPI.RegroupReplicasGTID)

	// Replication, general:
	httpAPI.registerAPIRequest(m, "enable-gtid/:host/:port", httpAPI.EnableGTID)
	httpAPI.registerAPIRequest(m, "disable-gtid/:host/:port", httpAPI.DisableGTID)
	httpAPI.registerAPIRequest(m, "locate-gtid-errant/:host/:port", httpAPI.LocateErrantGTID)
	httpAPI.registerAPIRequest(m, "gtid-errant-reset-primary/:host/:port", httpAPI.ErrantGTIDResetPrimary)
	httpAPI.registerAPIRequest(m, "gtid-errant-inject-empty/:host/:port", httpAPI.ErrantGTIDInjectEmpty)
	httpAPI.registerAPIRequest(m, "skip-query/:host/:port", httpAPI.SkipQuery)
	httpAPI.registerAPIRequest(m, "start-replica/:host/:port", httpAPI.StartReplication)
	httpAPI.registerAPIRequest(m, "restart-replica/:host/:port", httpAPI.RestartReplication)
	httpAPI.registerAPIRequest(m, "stop-replica/:host/:port", httpAPI.StopReplication)
	httpAPI.registerAPIRequest(m, "stop-replica-nice/:host/:port", httpAPI.StopReplicationNicely)
	httpAPI.registerAPIRequest(m, "reset-replica/:host/:port", httpAPI.ResetReplication)
	httpAPI.registerAPIRequest(m, "detach-replica/:host/:port", httpAPI.DetachReplicaPrimaryHost)
	httpAPI.registerAPIRequest(m, "reattach-replica/:host/:port", httpAPI.NoOp)
	httpAPI.registerAPIRequest(m, "detach-replica-primary-host/:host/:port", httpAPI.DetachReplicaPrimaryHost)
	httpAPI.registerAPIRequest(m, "reattach-replica-primary-host/:host/:port", httpAPI.NoOp)
	httpAPI.registerAPIRequest(m, "flush-binary-logs/:host/:port", httpAPI.FlushBinaryLogs)
	httpAPI.registerAPIRequest(m, "purge-binary-logs/:host/:port/:logFile", httpAPI.PurgeBinaryLogs)
	httpAPI.registerAPIRequest(m, "restart-replica-statements/:host/:port", httpAPI.RestartReplicationStatements)

	// Replication information:
	httpAPI.registerAPIRequest(m, "can-replicate-from/:host/:port/:belowHost/:belowPort", httpAPI.CanReplicateFrom)
	httpAPI.registerAPIRequest(m, "can-replicate-from-gtid/:host/:port/:belowHost/:belowPort", httpAPI.CanReplicateFromGTID)

	// Instance:
	httpAPI.registerAPIRequest(m, "set-read-only/:host/:port", httpAPI.SetReadOnly)
	httpAPI.registerAPIRequest(m, "set-writeable/:host/:port", httpAPI.SetWriteable)
	httpAPI.registerAPIRequest(m, "kill-query/:host/:port/:process", httpAPI.KillQuery)

	// Pools:
	httpAPI.registerAPIRequest(m, "submit-pool-instances/:pool", httpAPI.SubmitPoolInstances)
	httpAPI.registerAPIRequest(m, "cluster-pool-instances/:clusterName", httpAPI.ReadClusterPoolInstancesMap)
	httpAPI.registerAPIRequest(m, "cluster-pool-instances/:clusterName/:pool", httpAPI.ReadClusterPoolInstancesMap)
	httpAPI.registerAPIRequest(m, "heuristic-cluster-pool-instances/:clusterName", httpAPI.GetHeuristicClusterPoolInstances)
	httpAPI.registerAPIRequest(m, "heuristic-cluster-pool-instances/:clusterName/:pool", httpAPI.GetHeuristicClusterPoolInstances)
	httpAPI.registerAPIRequest(m, "heuristic-cluster-pool-lag/:clusterName", httpAPI.GetHeuristicClusterPoolInstancesLag)
	httpAPI.registerAPIRequest(m, "heuristic-cluster-pool-lag/:clusterName/:pool", httpAPI.GetHeuristicClusterPoolInstancesLag)

	// Information:
	httpAPI.registerAPIRequest(m, "search/:searchString", httpAPI.Search)
	httpAPI.registerAPIRequest(m, "search", httpAPI.Search)

	// Cluster
	httpAPI.registerAPIRequest(m, "cluster/:clusterHint", httpAPI.Cluster)
	httpAPI.registerAPIRequest(m, "cluster/alias/:clusterAlias", httpAPI.ClusterByAlias)
	httpAPI.registerAPIRequest(m, "cluster/instance/:host/:port", httpAPI.ClusterByInstance)
	httpAPI.registerAPIRequest(m, "cluster-info/:clusterHint", httpAPI.ClusterInfo)
	httpAPI.registerAPIRequest(m, "cluster-info/alias/:clusterAlias", httpAPI.ClusterInfoByAlias)
	httpAPI.registerAPIRequest(m, "cluster-osc-replicas/:clusterHint", httpAPI.ClusterOSCReplicas)
	httpAPI.registerAPIRequest(m, "set-cluster-alias/:clusterName", httpAPI.NoOp)
	httpAPI.registerAPIRequest(m, "clusters", httpAPI.Clusters)
	httpAPI.registerAPIRequest(m, "clusters-info", httpAPI.ClustersInfo)

	httpAPI.registerAPIRequest(m, "primaries", httpAPI.Primaries)
	httpAPI.registerAPIRequest(m, "primary/:clusterHint", httpAPI.ClusterPrimary)
	httpAPI.registerAPIRequest(m, "instance-replicas/:host/:port", httpAPI.InstanceReplicas)
	httpAPI.registerAPIRequest(m, "all-instances", httpAPI.AllInstances)
	httpAPI.registerAPIRequest(m, "downtimed", httpAPI.Downtimed)
	httpAPI.registerAPIRequest(m, "downtimed/:clusterHint", httpAPI.Downtimed)
	httpAPI.registerAPIRequest(m, "topology/:clusterHint", httpAPI.ASCIITopology)
	httpAPI.registerAPIRequest(m, "topology/:host/:port", httpAPI.ASCIITopology)
	httpAPI.registerAPIRequest(m, "topology-tabulated/:clusterHint", httpAPI.ASCIITopologyTabulated)
	httpAPI.registerAPIRequest(m, "topology-tabulated/:host/:port", httpAPI.ASCIITopologyTabulated)
	httpAPI.registerAPIRequest(m, "topology-tags/:clusterHint", httpAPI.ASCIITopologyTags)
	httpAPI.registerAPIRequest(m, "topology-tags/:host/:port", httpAPI.ASCIITopologyTags)
	httpAPI.registerAPIRequest(m, "snapshot-topologies", httpAPI.SnapshotTopologies)

	// Tags:
	httpAPI.registerAPIRequest(m, "tagged", httpAPI.Tagged)
	httpAPI.registerAPIRequest(m, "tags/:host/:port", httpAPI.Tags)
	httpAPI.registerAPIRequest(m, "tag-value/:host/:port", httpAPI.TagValue)
	httpAPI.registerAPIRequest(m, "tag-value/:host/:port/:tagName", httpAPI.TagValue)
	httpAPI.registerAPIRequest(m, "tag/:host/:port", httpAPI.Tag)
	httpAPI.registerAPIRequest(m, "tag/:host/:port/:tagName/:tagValue", httpAPI.Tag)
	httpAPI.registerAPIRequest(m, "untag/:host/:port", httpAPI.Untag)
	httpAPI.registerAPIRequest(m, "untag/:host/:port/:tagName", httpAPI.Untag)
	httpAPI.registerAPIRequest(m, "untag-all", httpAPI.UntagAll)
	httpAPI.registerAPIRequest(m, "untag-all/:tagName/:tagValue", httpAPI.UntagAll)

	// Instance management:
	httpAPI.registerAPIRequest(m, "instance/:host/:port", httpAPI.Instance)
	httpAPI.registerAPIRequest(m, "discover/:host/:port", httpAPI.Discover)
	httpAPI.registerAPIRequest(m, "async-discover/:host/:port", httpAPI.AsyncDiscover)
	httpAPI.registerAPIRequest(m, "refresh/:host/:port", httpAPI.Refresh)
	httpAPI.registerAPIRequest(m, "forget/:host/:port", httpAPI.Forget)
	httpAPI.registerAPIRequest(m, "forget-cluster/:clusterHint", httpAPI.ForgetCluster)
	httpAPI.registerAPIRequest(m, "begin-maintenance/:host/:port/:owner/:reason", httpAPI.BeginMaintenance)
	httpAPI.registerAPIRequest(m, "end-maintenance/:host/:port", httpAPI.EndMaintenanceByInstanceKey)
	httpAPI.registerAPIRequest(m, "in-maintenance/:host/:port", httpAPI.InMaintenance)
	httpAPI.registerAPIRequest(m, "end-maintenance/:maintenanceKey", httpAPI.EndMaintenance)
	httpAPI.registerAPIRequest(m, "maintenance", httpAPI.Maintenance)
	httpAPI.registerAPIRequest(m, "begin-downtime/:host/:port/:owner/:reason", httpAPI.BeginDowntime)
	httpAPI.registerAPIRequest(m, "begin-downtime/:host/:port/:owner/:reason/:duration", httpAPI.BeginDowntime)
	httpAPI.registerAPIRequest(m, "end-downtime/:host/:port", httpAPI.EndDowntime)

	// Recovery:
	httpAPI.registerAPIRequest(m, "replication-analysis", httpAPI.ReplicationAnalysis)
	httpAPI.registerAPIRequest(m, "replication-analysis/:clusterName", httpAPI.ReplicationAnalysisForCluster)
	httpAPI.registerAPIRequest(m, "replication-analysis/instance/:host/:port", httpAPI.ReplicationAnalysisForKey)
	httpAPI.registerAPIRequest(m, "recover/:host/:port", httpAPI.Recover)
	httpAPI.registerAPIRequest(m, "recover/:host/:port/:candidateHost/:candidatePort", httpAPI.Recover)
	httpAPI.registerAPIRequest(m, "recover-lite/:host/:port", httpAPI.RecoverLite)
	httpAPI.registerAPIRequest(m, "recover-lite/:host/:port/:candidateHost/:candidatePort", httpAPI.RecoverLite)
	httpAPI.registerAPIRequest(m, "graceful-primary-takeover/:host/:port", httpAPI.GracefulPrimaryTakeover)
	httpAPI.registerAPIRequest(m, "graceful-primary-takeover/:host/:port/:designatedHost/:designatedPort", httpAPI.GracefulPrimaryTakeover)
	httpAPI.registerAPIRequest(m, "graceful-primary-takeover/:clusterHint", httpAPI.GracefulPrimaryTakeover)
	httpAPI.registerAPIRequest(m, "graceful-primary-takeover/:clusterHint/:designatedHost/:designatedPort", httpAPI.GracefulPrimaryTakeover)
	httpAPI.registerAPIRequest(m, "graceful-primary-takeover-auto/:host/:port", httpAPI.GracefulPrimaryTakeoverAuto)
	httpAPI.registerAPIRequest(m, "graceful-primary-takeover-auto/:host/:port/:designatedHost/:designatedPort", httpAPI.GracefulPrimaryTakeoverAuto)
	httpAPI.registerAPIRequest(m, "graceful-primary-takeover-auto/:clusterHint", httpAPI.GracefulPrimaryTakeoverAuto)
	httpAPI.registerAPIRequest(m, "graceful-primary-takeover-auto/:clusterHint/:designatedHost/:designatedPort", httpAPI.GracefulPrimaryTakeoverAuto)
	httpAPI.registerAPIRequest(m, "force-primary-failover/:host/:port", httpAPI.ForcePrimaryFailover)
	httpAPI.registerAPIRequest(m, "force-primary-failover/:clusterHint", httpAPI.ForcePrimaryFailover)
	httpAPI.registerAPIRequest(m, "force-primary-takeover/:clusterHint/:designatedHost/:designatedPort", httpAPI.ForcePrimaryTakeover)
	httpAPI.registerAPIRequest(m, "force-primary-takeover/:host/:port/:designatedHost/:designatedPort", httpAPI.ForcePrimaryTakeover)
	httpAPI.registerAPIRequest(m, "register-candidate/:host/:port/:promotionRule", httpAPI.RegisterCandidate)
	httpAPI.registerAPIRequest(m, "automated-recovery-filters", httpAPI.AutomatedRecoveryFilters)
	httpAPI.registerAPIRequest(m, "audit-failure-detection", httpAPI.AuditFailureDetection)
	httpAPI.registerAPIRequest(m, "audit-failure-detection/:page", httpAPI.AuditFailureDetection)
	httpAPI.registerAPIRequest(m, "audit-failure-detection/id/:id", httpAPI.AuditFailureDetection)
	httpAPI.registerAPIRequest(m, "audit-failure-detection/alias/:clusterAlias", httpAPI.AuditFailureDetection)
	httpAPI.registerAPIRequest(m, "audit-failure-detection/alias/:clusterAlias/:page", httpAPI.AuditFailureDetection)
	httpAPI.registerAPIRequest(m, "replication-analysis-changelog", httpAPI.ReadReplicationAnalysisChangelog)
	httpAPI.registerAPIRequest(m, "audit-recovery", httpAPI.AuditRecovery)
	httpAPI.registerAPIRequest(m, "audit-recovery/:page", httpAPI.AuditRecovery)
	httpAPI.registerAPIRequest(m, "audit-recovery/id/:id", httpAPI.AuditRecovery)
	httpAPI.registerAPIRequest(m, "audit-recovery/uid/:uid", httpAPI.AuditRecovery)
	httpAPI.registerAPIRequest(m, "audit-recovery/cluster/:clusterName", httpAPI.AuditRecovery)
	httpAPI.registerAPIRequest(m, "audit-recovery/cluster/:clusterName/:page", httpAPI.AuditRecovery)
	httpAPI.registerAPIRequest(m, "audit-recovery/alias/:clusterAlias", httpAPI.AuditRecovery)
	httpAPI.registerAPIRequest(m, "audit-recovery/alias/:clusterAlias/:page", httpAPI.AuditRecovery)
	httpAPI.registerAPIRequest(m, "audit-recovery-steps/:uid", httpAPI.AuditRecoverySteps)
	httpAPI.registerAPIRequest(m, "active-cluster-recovery/:clusterName", httpAPI.ActiveClusterRecovery)
	httpAPI.registerAPIRequest(m, "recently-active-cluster-recovery/:clusterName", httpAPI.RecentlyActiveClusterRecovery)
	httpAPI.registerAPIRequest(m, "recently-active-instance-recovery/:host/:port", httpAPI.RecentlyActiveInstanceRecovery)
	httpAPI.registerAPIRequest(m, "ack-recovery/cluster/:clusterHint", httpAPI.AcknowledgeClusterRecoveries)
	httpAPI.registerAPIRequest(m, "ack-recovery/cluster/alias/:clusterAlias", httpAPI.AcknowledgeClusterRecoveries)
	httpAPI.registerAPIRequest(m, "ack-recovery/instance/:host/:port", httpAPI.AcknowledgeInstanceRecoveries)
	httpAPI.registerAPIRequest(m, "ack-recovery/:recoveryId", httpAPI.AcknowledgeRecovery)
	httpAPI.registerAPIRequest(m, "ack-recovery/uid/:uid", httpAPI.AcknowledgeRecovery)
	httpAPI.registerAPIRequest(m, "ack-all-recoveries", httpAPI.AcknowledgeAllRecoveries)
	httpAPI.registerAPIRequest(m, "blocked-recoveries", httpAPI.BlockedRecoveries)
	httpAPI.registerAPIRequest(m, "blocked-recoveries/cluster/:clusterName", httpAPI.BlockedRecoveries)
	httpAPI.registerAPIRequest(m, "disable-global-recoveries", httpAPI.DisableGlobalRecoveries)
	httpAPI.registerAPIRequest(m, "enable-global-recoveries", httpAPI.EnableGlobalRecoveries)
	httpAPI.registerAPIRequest(m, "check-global-recoveries", httpAPI.CheckGlobalRecoveries)

	// General
	httpAPI.registerAPIRequest(m, "problems", httpAPI.Problems)
	httpAPI.registerAPIRequest(m, "problems/:clusterName", httpAPI.Problems)
	httpAPI.registerAPIRequest(m, "audit", httpAPI.Audit)
	httpAPI.registerAPIRequest(m, "audit/:page", httpAPI.Audit)
	httpAPI.registerAPIRequest(m, "audit/instance/:host/:port", httpAPI.Audit)
	httpAPI.registerAPIRequest(m, "audit/instance/:host/:port/:page", httpAPI.Audit)
	httpAPI.registerAPIRequest(m, "resolve/:host/:port", httpAPI.Resolve)

	// Meta, no proxy
	httpAPI.registerAPIRequestNoProxy(m, "headers", httpAPI.Headers)
	httpAPI.registerAPIRequestNoProxy(m, "health", httpAPI.Health)
	httpAPI.registerAPIRequestNoProxy(m, "lb-check", httpAPI.LBCheck)
	httpAPI.registerAPIRequestNoProxy(m, "_ping", httpAPI.LBCheck)
	httpAPI.registerAPIRequestNoProxy(m, "leader-check", httpAPI.LeaderCheck)
	httpAPI.registerAPIRequestNoProxy(m, "leader-check/:errorStatusCode", httpAPI.LeaderCheck)
	httpAPI.registerAPIRequestNoProxy(m, "grab-election", httpAPI.GrabElection)
	httpAPI.registerAPIRequestNoProxy(m, "reload-configuration", httpAPI.ReloadConfiguration)
	httpAPI.registerAPIRequestNoProxy(m, "hostname-resolve-cache", httpAPI.HostnameResolveCache)
	httpAPI.registerAPIRequestNoProxy(m, "reset-hostname-resolve-cache", httpAPI.ResetHostnameResolveCache)
	// Meta
	httpAPI.registerAPIRequest(m, "routed-leader-check", httpAPI.LeaderCheck)
	httpAPI.registerAPIRequest(m, "reelect", httpAPI.Reelect)
	httpAPI.registerAPIRequest(m, "reload-cluster-alias", httpAPI.ReloadClusterAlias)
	httpAPI.registerAPIRequest(m, "deregister-hostname-unresolve/:host/:port", httpAPI.DeregisterHostnameUnresolve)
	httpAPI.registerAPIRequest(m, "register-hostname-unresolve/:host/:port/:virtualname", httpAPI.RegisterHostnameUnresolve)

	// Bulk access to information
	httpAPI.registerAPIRequest(m, "bulk-instances", httpAPI.BulkInstances)
	httpAPI.registerAPIRequest(m, "bulk-promotion-rules", httpAPI.BulkPromotionRules)

	// Monitoring
	httpAPI.registerAPIRequest(m, "discovery-metrics-raw/:seconds", httpAPI.DiscoveryMetricsRaw)
	httpAPI.registerAPIRequest(m, "discovery-metrics-aggregated/:seconds", httpAPI.DiscoveryMetricsAggregated)
	httpAPI.registerAPIRequest(m, "discovery-queue-metrics-raw/:seconds", httpAPI.DiscoveryQueueMetricsRaw)
	httpAPI.registerAPIRequest(m, "discovery-queue-metrics-aggregated/:seconds", httpAPI.DiscoveryQueueMetricsAggregated)
	httpAPI.registerAPIRequest(m, "backend-query-metrics-raw/:seconds", httpAPI.BackendQueryMetricsRaw)
	httpAPI.registerAPIRequest(m, "backend-query-metrics-aggregated/:seconds", httpAPI.BackendQueryMetricsAggregated)
	httpAPI.registerAPIRequest(m, "write-buffer-metrics-raw/:seconds", httpAPI.WriteBufferMetricsRaw)
	httpAPI.registerAPIRequest(m, "write-buffer-metrics-aggregated/:seconds", httpAPI.WriteBufferMetricsAggregated)

	// Configurable status check endpoint
	if config.Config.StatusEndpoint == config.DefaultStatusAPIEndpoint {
		httpAPI.registerAPIRequestNoProxy(m, "status", httpAPI.StatusCheck)
	} else {
		m.Get(config.Config.StatusEndpoint, httpAPI.StatusCheck)
	}
}
