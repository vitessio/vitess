// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topo

import (
	"fmt"
	"strings"

	"github.com/youtube/vitess/go/vt/topo/topoproto"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// This file contains the methods to convert topo structures to proto3.
// Eventually we will use the proto3 data structures directly.

// TabletTypeToProto turns a TabletType into a proto
func TabletTypeToProto(t TabletType) pb.TabletType {
	if result, err := topoproto.ParseTabletType(string(t)); err != nil {
		panic(fmt.Errorf("unknown tablet type: %v", t))
	} else {
		return result
	}
}

// ProtoToTabletType turns a proto to a TabletType
func ProtoToTabletType(t pb.TabletType) TabletType {
	return TabletType(strings.ToLower(pb.TabletType_name[int32(t)]))
}
