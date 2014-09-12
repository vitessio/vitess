// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

// GTIDSet represents the set of transactions received or applied by a server.
// In some flavors, a single GTID is enough to specify the set of all
// transactions that came before it, but in others a more complex structure is
// required.
//
// GTIDSet is wrapped by ReplicationPosition, which is a concrete struct that
// enables JSON and BSON marshaling. Most code outside of this package should
// use ReplicationPosition rather than GTIDSet.
type GTIDSet interface {
	// String returns the canonical printed form of the set as expected by a
	// particular flavor of MySQL.
	String() string

	// Flavor returns the key under which the corresponding parser function is
	// registered in the transactionSetParsers map.
	Flavor() string

	// Last returns the GTID of the most recent transaction in the set.
	Last() GTID

	// Contains returns true if the set contains the specified transaction.
	ContainsGTID(GTID) bool

	// Contains returns true if the set is a superset of another set.
	Contains(GTIDSet) bool

	// Equal returns true if the set is equal to another set.
	Equal(GTIDSet) bool

	// AddGTID returns a new GTIDSet that is expanded to contain the given GTID.
	AddGTID(GTID) GTIDSet
}

// gtidSetParsers maps flavor names to parser functions. It is used by
// ParseReplicationPosition().
var gtidSetParsers = make(map[string]func(string) (GTIDSet, error))
