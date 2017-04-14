package replication

// GTIDSet represents the set of transactions received or applied by a server.
// In some flavors, a single GTID is enough to specify the set of all
// transactions that came before it, but in others a more complex structure is
// required.
//
// GTIDSet is wrapped by replication.Position, which is a concrete struct.
// When sending a GTIDSet over RPCs, encode/decode it as a string.
// Most code outside of this package should use replication.Position rather
// than GTIDSet.
type GTIDSet interface {
	// String returns the canonical printed form of the set as expected by a
	// particular flavor of MySQL.
	String() string

	// Flavor returns the key under which the corresponding parser function is
	// registered in the transactionSetParsers map.
	Flavor() string

	// ContainsGTID returns true if the set contains the specified transaction.
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
