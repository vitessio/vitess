// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package topoproto contains utility functions to deal with the proto3
// structures defined in proto/topodata.
package topoproto

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/youtube/vitess/go/netutil"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains the topodata.Tablet utility functions.

const (
	// Default name for databases is the prefix plus keyspace
	vtDbPrefix = "vt_"
)

// TabletAliasIsZero returns true iff cell and uid are empty
func TabletAliasIsZero(ta *pb.TabletAlias) bool {
	return ta == nil || (ta.Cell == "" && ta.Uid == 0)
}

// TabletAliasEqual returns true if two TabletAlias match
func TabletAliasEqual(left, right *pb.TabletAlias) bool {
	if left == nil {
		return right == nil
	}
	if right == nil {
		return false
	}
	return *left == *right
}

// TabletAliasString formats a TabletAlias
func TabletAliasString(ta *pb.TabletAlias) string {
	if ta == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%v-%010d", ta.Cell, ta.Uid)
}

// TabletAliasUIDStr returns a string version of the uid
func TabletAliasUIDStr(ta *pb.TabletAlias) string {
	return fmt.Sprintf("%010d", ta.Uid)
}

// ParseTabletAlias returns a TabletAlias for the input string,
// of the form <cell>-<uid>
func ParseTabletAlias(aliasStr string) (*pb.TabletAlias, error) {
	nameParts := strings.Split(aliasStr, "-")
	if len(nameParts) != 2 {
		return nil, fmt.Errorf("invalid tablet alias: %v", aliasStr)
	}
	uid, err := ParseUID(nameParts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid tablet uid %v: %v", aliasStr, err)
	}
	return &pb.TabletAlias{
		Cell: nameParts[0],
		Uid:  uid,
	}, nil
}

// ParseUID parses just the uid (a number)
func ParseUID(value string) (uint32, error) {
	uid, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("bad tablet uid %v", err)
	}
	return uint32(uid), nil
}

// TabletAliasList is used mainly for sorting
type TabletAliasList []*pb.TabletAlias

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
var AllTabletTypes = []pb.TabletType{
	pb.TabletType_MASTER,
	pb.TabletType_REPLICA,
	pb.TabletType_RDONLY,
	pb.TabletType_BATCH,
	pb.TabletType_SPARE,
	pb.TabletType_EXPERIMENTAL,
	pb.TabletType_BACKUP,
	pb.TabletType_RESTORE,
	pb.TabletType_WORKER,
}

// SlaveTabletTypes contains all the tablet type that can have replication
// enabled.
var SlaveTabletTypes = []pb.TabletType{
	pb.TabletType_REPLICA,
	pb.TabletType_RDONLY,
	pb.TabletType_BATCH,
	pb.TabletType_SPARE,
	pb.TabletType_EXPERIMENTAL,
	pb.TabletType_BACKUP,
	pb.TabletType_RESTORE,
	pb.TabletType_WORKER,
}

// ParseTabletType parses the tablet type into the enum
func ParseTabletType(param string) (pb.TabletType, error) {
	value, ok := pb.TabletType_value[strings.ToUpper(param)]
	if !ok {
		return pb.TabletType_UNKNOWN, fmt.Errorf("unknown TabletType %v", param)
	}
	return pb.TabletType(value), nil
}

// IsTypeInList returns true if the given type is in the list.
// Use it with AllTabletType and SlaveTabletType for instance.
func IsTypeInList(tabletType pb.TabletType, types []pb.TabletType) bool {
	for _, t := range types {
		if tabletType == t {
			return true
		}
	}
	return false
}

// MakeStringTypeList returns a list of strings that match the input list.
func MakeStringTypeList(types []pb.TabletType) []string {
	strs := make([]string, len(types))
	for i, t := range types {
		strs[i] = strings.ToLower(t.String())
	}
	sort.Strings(strs)
	return strs
}

// TabletAddr returns hostname:vt port associated with a tablet
func TabletAddr(tablet *pb.Tablet) string {
	return netutil.JoinHostPort(tablet.Hostname, tablet.PortMap["vt"])
}

// TabletDbName is usually implied by keyspace. Having the shard
// information in the database name complicates mysql replication.
func TabletDbName(tablet *pb.Tablet) string {
	if tablet.DbNameOverride != "" {
		return tablet.DbNameOverride
	}
	if tablet.Keyspace == "" {
		return ""
	}
	return vtDbPrefix + tablet.Keyspace
}

// TabletIsAssigned returns if this tablet is assigned to a keyspace and shard.
// A "scrap" node will show up as assigned even though its data cannot be used
// for serving.
func TabletIsAssigned(tablet *pb.Tablet) bool {
	return tablet != nil && tablet.Keyspace != "" && tablet.Shard != ""
}
