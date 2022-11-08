package viperutil

import (
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/viperutil/v2/internal/value"
)

var (
	_ Value[int] = (*value.Static[int])(nil)
	_ Value[int] = (*value.Dynamic[int])(nil)
)

type Value[T any] interface {
	value.Registerable
	Get() T
	Default() T
}

func BindFlags(fs *pflag.FlagSet, values ...value.Registerable) {
	value.BindFlags(fs, values...)
}
