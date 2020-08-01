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

// Package topoproto contains utility functions to deal with the proto3
// structures defined in proto/topodata.
package topoproto

import (
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	"vitess.io/vitess/go/netutil"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This file contains the topodata.Tablet utility functions.

const (
	// VtDbPrefix + keyspace is the default name for databases.
	VtDbPrefix = "vt_"
)

// cache the conversion from tablet type enum to lower case string.
var tabletTypeLowerName map[int32]string

func init() {
	tabletTypeLowerName = make(map[int32]string, len(topodatapb.TabletType_name))
	for k, v := range topodatapb.TabletType_name {
		tabletTypeLowerName[k] = strings.ToLower(v)
	}
}

// TabletAliasIsZero returns true iff cell and uid are empty
func TabletAliasIsZero(ta *topodatapb.TabletAlias) bool {
	return ta == nil || (ta.Cell == "" && ta.Uid == 0)
}

// TabletAliasEqual returns true if two TabletAlias match
func TabletAliasEqual(left, right *topodatapb.TabletAlias) bool {
	return proto.Equal(left, right)
}

// TabletAliasString formats a TabletAlias
func TabletAliasString(ta *topodatapb.TabletAlias) string {
	if ta == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%v-%010d", ta.Cell, ta.Uid)
}

// TabletAliasUIDStr returns a string version of the uid
func TabletAliasUIDStr(ta *topodatapb.TabletAlias) string {
	return fmt.Sprintf("%010d", ta.Uid)
}

// ParseTabletAlias returns a TabletAlias for the input string,
// of the form <cell>-<uid>
func ParseTabletAlias(aliasStr string) (*topodatapb.TabletAlias, error) {
	nameParts := strings.Split(aliasStr, "-")
	if len(nameParts) != 2 {
		return nil, fmt.Errorf("invalid tablet alias: '%s', expecting format: '<cell>-<uid>'", aliasStr)
	}
	uid, err := ParseUID(nameParts[1])
	if err != nil {
		return nil, vterrors.Wrapf(err, "invalid tablet uid in alias '%s'", aliasStr)
	}
	return &topodatapb.TabletAlias{
		Cell: nameParts[0],
		Uid:  uid,
	}, nil
}

// ParseUID parses just the uid (a number)
func ParseUID(value string) (uint32, error) {
	uid, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, vterrors.Wrap(err, "bad tablet uid")
	}
	return uint32(uid), nil
}

// TabletAliasList is used mainly for sorting
type TabletAliasList []*topodatapb.TabletAlias

// Len is part of sort.Interface
func (tal TabletAliasList) Len() int {
	return len(tal)
}

// Less is part of sort.Interface
func (tal TabletAliasList) Less(i, j int) bool {
	if tal[i].Cell < tal[j].Cell {
		return true
	} else if tal[i].Cell > tal[j].Cell {
		return false
	}
	return tal[i].Uid < tal[j].Uid
}

// Swap is part of sort.Interface
func (tal TabletAliasList) Swap(i, j int) {
	tal[i], tal[j] = tal[j], tal[i]
}

// AllTabletTypes lists all the possible tablet types
var AllTabletTypes = []topodatapb.TabletType{
	topodatapb.TabletType_MASTER,
	topodatapb.TabletType_REPLICA,
	topodatapb.TabletType_RDONLY,
	topodatapb.TabletType_BATCH,
	topodatapb.TabletType_SPARE,
	topodatapb.TabletType_EXPERIMENTAL,
	topodatapb.TabletType_BACKUP,
	topodatapb.TabletType_RESTORE,
	topodatapb.TabletType_DRAINED,
}

// ParseTabletType parses the tablet type into the enum.
func ParseTabletType(param string) (topodatapb.TabletType, error) {
	value, ok := topodatapb.TabletType_value[strings.ToUpper(param)]
	if !ok {
		return topodatapb.TabletType_UNKNOWN, fmt.Errorf("unknown TabletType %v", param)
	}
	return topodatapb.TabletType(value), nil
}

// ParseTabletTypes parses a comma separated list of tablet types and returns a slice with the respective enums.
func ParseTabletTypes(param string) ([]topodatapb.TabletType, error) {
	var tabletTypes []topodatapb.TabletType
	for _, typeStr := range strings.Split(param, ",") {
		t, err := ParseTabletType(typeStr)
		if err != nil {
			return nil, err
		}
		tabletTypes = append(tabletTypes, t)
	}
	return tabletTypes, nil
}

// TabletTypeLString returns a lower case version of the tablet type,
// or "unknown" if not known.
func TabletTypeLString(tabletType topodatapb.TabletType) string {
	value, ok := tabletTypeLowerName[int32(tabletType)]
	if !ok {
		return "unknown"
	}
	return value
}

// IsTypeInList returns true if the given type is in the list.
// Use it with AllTabletTypes for instance.
func IsTypeInList(tabletType topodatapb.TabletType, types []topodatapb.TabletType) bool {
	for _, t := range types {
		if tabletType == t {
			return true
		}
	}
	return false
}

// MakeStringTypeList returns a list of strings that match the input list.
func MakeStringTypeList(types []topodatapb.TabletType) []string {
	strs := make([]string, len(types))
	for i, t := range types {
		strs[i] = strings.ToLower(t.String())
	}
	sort.Strings(strs)
	return strs
}

// MysqlAddr returns the host:port of the mysql server.
func MysqlAddr(tablet *topodatapb.Tablet) string {
	return netutil.JoinHostPort(tablet.MysqlHostname, tablet.MysqlPort)
}

// MySQLIP returns the MySQL server's IP by resolvign the host name.
func MySQLIP(tablet *topodatapb.Tablet) (string, error) {
	ipAddrs, err := net.LookupHost(tablet.MysqlHostname)
	if err != nil {
		return "", err
	}
	return ipAddrs[0], nil
}

// TabletDbName is usually implied by keyspace. Having the shard
// information in the database name complicates mysql replication.
func TabletDbName(tablet *topodatapb.Tablet) string {
	if tablet.DbNameOverride != "" {
		return tablet.DbNameOverride
	}
	if tablet.Keyspace == "" {
		return ""
	}
	return VtDbPrefix + tablet.Keyspace
}

// TabletIsAssigned returns if this tablet is assigned to a keyspace and shard.
// A "scrap" node will show up as assigned even though its data cannot be used
// for serving.
func TabletIsAssigned(tablet *topodatapb.Tablet) bool {
	return tablet != nil && tablet.Keyspace != "" && tablet.Shard != ""
}
