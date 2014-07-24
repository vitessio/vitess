package mysqlctl

// GTID represents a Global Transaction ID, also known as Transaction Group ID.
// Each flavor of MySQL has its own format for the GTID. This interface is used
// along with various MysqlFlavor implementations to abstract the differences.
//
// Types that implement GTID should use a non-pointer receiver. This ensures
// that comparing GTID interface values with == has the expected semantics.
type GTID interface {
	// String returns the canonical form of the GTID as expected by a particular
	// flavor of MySQL.
	String() string

	// TryCompare tries to compare two GTIDs. Some flavors of GTID can always be
	// compared (e.g. Google MySQL group_id). Others can only be compared if they
	// came from the same master (e.g. MariaDB, MySQL 5.6).
	//
	// If the comparison is possible, a.TryCompare(b) will return an int that is:
	//    < 0  if a < b  (a came before b)
	//   == 0  if a == b
	//    > 0  if a > b  (a came after b)
	//
	// If the comparison is not possible, a non-nil error will be returned.
	TryCompare(GTID) (int, error)
}
