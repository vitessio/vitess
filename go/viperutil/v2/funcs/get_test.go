package funcs_test

import (
	"fmt"

	"github.com/spf13/viper"

	"vitess.io/vitess/go/viperutil/v2"
	"vitess.io/vitess/go/viperutil/v2/funcs"
	"vitess.io/vitess/go/viperutil/v2/internal/value"
)

func ExampleGetPath() {
	v := viper.New()

	val := viperutil.Configure("path", viperutil.Options[[]string]{
		GetFunc: funcs.GetPath,
	})

	stub(val, v)

	v.Set(val.Key(), []string{"/var/www", "/usr:/usr/bin", "/vt"})
	fmt.Println(val.Get())
	// Output: [/var/www /usr /usr/bin /vt]
}

func stub[T any](val viperutil.Value[T], v *viper.Viper) {
	// N.B.: You cannot do this in normal code because these types are internal
	// to viperutil, but you also will not need to do this. However it's
	// necessary for the example to work here.
	base := val.(*value.Static[T]).Base
	base.BoundGetFunc = base.GetFunc(v)
}
