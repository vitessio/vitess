// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

// Binlog server / player replication structures

import (
	"fmt"

	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"

	pbt "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
)

// BlpPosition describes a binlog player position to start from.
type BlpPosition struct {
	Uid      uint32
	Position myproto.ReplicationPosition
}

//go:generate bsongen -file $GOFILE -type BlpPosition -o blp_position_bson.go

// BlpPositionList is a list of BlpPosition, not sorted.
type BlpPositionList struct {
	Entries []BlpPosition
}

//go:generate bsongen -file $GOFILE -type BlpPositionList -o blp_position_list_bson.go

// FindBlpPositionById returns the BlpPosition with the given id, or error
func (bpl *BlpPositionList) FindBlpPositionById(id uint32) (*BlpPosition, error) {
	for _, pos := range bpl.Entries {
		if pos.Uid == id {
			return &pos, nil
		}
	}
	return nil, fmt.Errorf("BlpPosition for id %v not found", id)
}

// FindBlpPositionById2 returns the BlpPosition with the given id, or error
func FindBlpPositionById(list []*pbt.BlpPosition, uid uint32) *pbt.BlpPosition {
	for _, pos := range list {
		if pos.Uid == uid {
			return pos
		}
	}
	return nil
}
