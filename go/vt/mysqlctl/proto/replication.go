// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/bytes2"
	"github.com/youtube/vitess/go/netutil"
)

// ReplicationPosition represents the information necessary to describe which
// transactions a server has seen, so that it can request a replication stream
// from a new master that picks up where it left off.
//
// This must be a concrete struct because custom Unmarshalers can't be
// registered on an interface.
//
// The == operator should not be used with ReplicationPosition, because the
// underlying GTIDSet might use slices, which are not comparable. Using == in
// those cases will result in a run-time panic.
type ReplicationPosition struct {
	GTIDSet GTIDSet

	// This is a zero byte compile-time check that no one is trying to
	// use == or != with ReplicationPosition. Without this, we won't know there's
	// a problem until the runtime panic.
	_ [0]struct{ notComparable []byte }
}

// Equal returns true if this position is equal to another.
func (rp ReplicationPosition) Equal(other ReplicationPosition) bool {
	if rp.GTIDSet == nil {
		return other.GTIDSet == nil
	}
	return rp.GTIDSet.Equal(other.GTIDSet)
}

// AtLeast returns true if this position is equal to or after another.
func (rp ReplicationPosition) AtLeast(other ReplicationPosition) bool {
	if rp.GTIDSet == nil {
		return other.GTIDSet == nil
	}
	return rp.GTIDSet.Contains(other.GTIDSet)
}

// String returns a string representation of the underlying GTIDSet.
// If the set is nil, it returns "<nil>" in the style of Sprintf("%v", nil).
func (rp ReplicationPosition) String() string {
	if rp.GTIDSet == nil {
		return "<nil>"
	}
	return rp.GTIDSet.String()
}

// IsZero returns true if this is the zero value, ReplicationPosition{}.
func (rp ReplicationPosition) IsZero() bool {
	return rp.GTIDSet == nil
}

// AppendGTID returns a new ReplicationPosition that represents the position
// after the given GTID is replicated.
func AppendGTID(rp ReplicationPosition, gtid GTID) ReplicationPosition {
	if gtid == nil {
		return rp
	}
	if rp.GTIDSet == nil {
		return ReplicationPosition{GTIDSet: gtid.GTIDSet()}
	}
	return ReplicationPosition{GTIDSet: rp.GTIDSet.AddGTID(gtid)}
}

// MustParseReplicationPosition calls ParseReplicationPosition and panics
// on error.
func MustParseReplicationPosition(flavor, value string) ReplicationPosition {
	rp, err := ParseReplicationPosition(flavor, value)
	if err != nil {
		panic(err)
	}
	return rp
}

// EncodeReplicationPosition returns a string that contains both the flavor
// and value of the ReplicationPosition, so that the correct parser can be
// selected when that string is passed to DecodeReplicationPosition.
func EncodeReplicationPosition(rp ReplicationPosition) string {
	if rp.GTIDSet == nil {
		return ""
	}
	return fmt.Sprintf("%s/%s", rp.GTIDSet.Flavor(), rp.GTIDSet.String())
}

// DecodeReplicationPosition converts a string in the format returned by
// EncodeReplicationPosition back into a ReplicationPosition value with the
// correct underlying flavor.
func DecodeReplicationPosition(s string) (rp ReplicationPosition, err error) {
	if s == "" {
		return rp, nil
	}

	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 {
		// There is no flavor. Try looking for a default parser.
		return ParseReplicationPosition("", s)
	}
	return ParseReplicationPosition(parts[0], parts[1])
}

// ParseReplicationPosition calls the parser for the specified flavor.
func ParseReplicationPosition(flavor, value string) (rp ReplicationPosition, err error) {
	parser := gtidSetParsers[flavor]
	if parser == nil {
		return rp, fmt.Errorf("parse error: unknown GTIDSet flavor %#v", flavor)
	}
	gtidSet, err := parser(value)
	if err != nil {
		return rp, err
	}
	rp.GTIDSet = gtidSet
	return rp, err
}

// MarshalBson bson-encodes ReplicationPosition.
func (rp ReplicationPosition) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)

	lenWriter := bson.NewLenWriter(buf)

	if rp.GTIDSet != nil {
		// The name of the bson field is the MySQL flavor.
		bson.EncodeString(buf, rp.GTIDSet.Flavor(), rp.GTIDSet.String())
	}

	lenWriter.Close()
}

// UnmarshalBson bson-decodes into ReplicationPosition.
func (rp *ReplicationPosition) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	switch kind {
	case bson.EOO, bson.Object:
		// valid
	case bson.Null:
		return
	default:
		panic(bson.NewBsonError("unexpected kind %v for ReplicationPosition", kind))
	}
	bson.Next(buf, 4)

	// We expect exactly zero or one fields in this bson object.
	kind = bson.NextByte(buf)
	if kind == bson.EOO {
		// The value was nil, nothing to do.
		return
	}

	// The field name is the MySQL flavor.
	flavor := bson.ReadCString(buf)
	value := bson.DecodeString(buf, kind)

	// Check for and consume the end byte.
	if kind = bson.NextByte(buf); kind != bson.EOO {
		panic(bson.NewBsonError("too many fields for ReplicationPosition"))
	}

	// Parse the value.
	var err error
	*rp, err = ParseReplicationPosition(flavor, value)
	if err != nil {
		panic(bson.NewBsonError("invalid value %#v for ReplicationPosition: %v", value, err))
	}
}

// MarshalJSON implements encoding/json.Marshaler.
func (rp ReplicationPosition) MarshalJSON() ([]byte, error) {
	return json.Marshal(EncodeReplicationPosition(rp))
}

// UnmarshalJSON implements encoding/json.Unmarshaler.
func (rp *ReplicationPosition) UnmarshalJSON(buf []byte) error {
	var s string
	err := json.Unmarshal(buf, &s)
	if err != nil {
		return err
	}

	*rp, err = DecodeReplicationPosition(s)
	if err != nil {
		return err
	}
	return nil
}

// ReplicationStatus holds replication information from SHOW SLAVE STATUS.
type ReplicationStatus struct {
	Position            ReplicationPosition
	SlaveIORunning      bool
	SlaveSQLRunning     bool
	SecondsBehindMaster uint
	MasterHost          string
	MasterPort          int
	MasterConnectRetry  int
}

// SlaveRunning returns true iff both the Slave IO and Slave SQL threads are
// running.
func (rs *ReplicationStatus) SlaveRunning() bool {
	return rs.SlaveIORunning && rs.SlaveSQLRunning
}

// MasterAddr returns the host:port address of the master.
func (rs *ReplicationStatus) MasterAddr() string {
	return fmt.Sprintf("%v:%v", rs.MasterHost, rs.MasterPort)
}

// NewReplicationStatus creates a ReplicationStatus pointing to masterAddr.
func NewReplicationStatus(masterAddr string) (*ReplicationStatus, error) {
	host, port, err := netutil.SplitHostPort(masterAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid masterAddr: %q, %v", masterAddr, err)
	}
	return &ReplicationStatus{MasterConnectRetry: 10,
		MasterHost: host, MasterPort: port}, nil
}
