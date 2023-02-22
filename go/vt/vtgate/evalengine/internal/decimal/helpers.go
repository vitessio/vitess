package decimal

import "vitess.io/vitess/go/vt/vthash"

func (d *Decimal) Hash(hasher *vthash.Hasher) {
	_, _ = hasher.Write(d.formatFast(0, false, true))
}
