package mysqlctl

type fakeGTID struct{}

func (fakeGTID) String() string               { return "" }
func (fakeGTID) TryCompare(GTID) (int, error) { return 0, nil }
