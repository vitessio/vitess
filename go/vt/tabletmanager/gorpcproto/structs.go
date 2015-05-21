// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcproto

import (
	"time"

	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/topo"
)

/*

This file contains the structures used to pack the RPC args and reply
for the gorpc transport for tablet manager.

Arguments are composed of the name of the call + Args.
Replies are composed of the name of the call + Reply.
If a struct is used as both Arguments and Replies, use name of the call + Data.

NOTE(alainjobart) It is OK to rename a structure, as the type is not
passed in through the RPC. I tested that by creating another set of
structures here, adding a '2' to the end of the types, and changing
gorpc_server.go to only use the '2' structures. Everything worked the same.

*/

// PopulateReparentJournalArgs has arguments for PopulateReparentJournal
type PopulateReparentJournalArgs struct {
	TimeCreatedNS       int64
	ActionName          string
	MasterAlias         topo.TabletAlias
	ReplicationPosition myproto.ReplicationPosition
}

// InitSlaveArgs has arguments for InitSlave
type InitSlaveArgs struct {
	Parent              topo.TabletAlias
	ReplicationPosition myproto.ReplicationPosition
	TimeCreatedNS       int64
	WaitTimeout         time.Duration // pass in zero to wait indefinitely
}

// SetMasterArgs has arguments for SetMaster
type SetMasterArgs struct {
	Parent          topo.TabletAlias
	TimeCreatedNS   int64
	ForceStartSlave bool
	WaitTimeout     time.Duration // pass in zero to wait indefinitely
}

// GetSchemaArgs has arguments for GetSchema
type GetSchemaArgs struct {
	Tables        []string
	ExcludeTables []string
	IncludeViews  bool
}

// StopSlaveMinimumArgs has arguments for StopSlaveMinimum
type StopSlaveMinimumArgs struct {
	Position myproto.ReplicationPosition
	WaitTime time.Duration
}

// GetSlavesReply has the reply for GetSlaves
type GetSlavesReply struct {
	Addrs []string
}

// WaitBlpPositionArgs has arguments for WaitBlpPosition
type WaitBlpPositionArgs struct {
	BlpPosition blproto.BlpPosition
	WaitTimeout time.Duration
}

// RunBlpUntilArgs has arguments for RunBlpUntil
type RunBlpUntilArgs struct {
	BlpPositionList *blproto.BlpPositionList
	WaitTimeout     time.Duration
}

// ExecuteFetchArgs has arguments for ExecuteFetch
type ExecuteFetchArgs struct {
	Query          string
	DbName         string
	MaxRows        int
	WantFields     bool
	DisableBinlogs bool
	ReloadSchema   bool
}

// BackupArgs has arguments for Backup
type BackupArgs struct {
	Concurrency int
}

// TabletExternallyReparentedArgs has arguments for TabletExternallyReparented
type TabletExternallyReparentedArgs struct {
	ExternalID string
}
