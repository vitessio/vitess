package topodata

import (
	"strconv"
)

// Set is part of the pflag.Value interface.
func (tt *TabletType) Set(s string) error {
	if val, err := strconv.ParseInt(s, 10, 32); err != nil {
		return err
	} else {
		*tt = TabletType(val)
		return nil
	}
}

// String is part of the pflag.Value interface.
func (tt *TabletType) String() string {
	return strconv.Itoa(int(*tt))
}

// Type is part of the pflag.Value interface.
func (tt *TabletType) Type() string {
	return "TabletType"
}
