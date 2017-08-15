package topoproto

import (
	"flag"
	"strings"

	"github.com/youtube/vitess/go/vt/proto/topodata"
)

// TabletTypeListVar defines a []TabletType flag with the specified name and usage
// string. The argument 'p' points to a []TabletType in which to store the value of the flag.
func TabletTypeListVar(p *[]topodata.TabletType, name string, usage string) {
	flag.Var((*TabletTypeListValue)(p), name, usage)
}

// TabletTypeVar defines a TabletType flag with the specified name, default value and usage
// string. The argument 'p' points to a tabletType in which to store the value of the flag.
func TabletTypeVar(p *topodata.TabletType, name string, defaultValue topodata.TabletType, usage string) {
	*p = defaultValue
	flag.Var((*TabletTypeFlag)(p), name, usage)
}

// TabletTypeListValue implements the flag.Value interface, for parsing a command-line comma-separated
// list of value into a slice of TabletTypes.
type TabletTypeListValue []topodata.TabletType

// String is part of the flag.Value interface.
func (ttlv *TabletTypeListValue) String() string {
	return strings.Join(MakeStringTypeList(*ttlv), ",")
}

// Set is part of the flag.Value interface.
func (ttlv *TabletTypeListValue) Set(v string) (err error) {
	*ttlv, err = ParseTabletTypes(v)
	return err
}

// TabletTypeFlag implements the flag.Value interface, for parsing a command-line value into a TabletType.
type TabletTypeFlag topodata.TabletType

// String is part of the flag.Value interface.
func (ttf *TabletTypeFlag) String() string {
	return topodata.TabletType(*ttf).String()
}

// Set is part of the flag.Value interface.
func (ttf *TabletTypeFlag) Set(v string) error {
	t, err := ParseTabletType(v)
	*ttf = TabletTypeFlag(t)
	return err
}
