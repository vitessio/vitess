package vipertest

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/viperutil/v2"
	"vitess.io/vitess/go/viperutil/v2/internal/value"
)

func Stub[T any](t *testing.T, v *viper.Viper, val viperutil.Value[T]) (reset func()) {
	t.Helper()

	reset = func() {}
	if !assert.False(t, v.InConfig(val.Key()), "value for key %s already stubbed", val.Key()) {
		return func() {}
	}

	var base *value.Base[T]
	switch val := val.(type) {
	case *value.Static[T]:
		base = val.Base
	case *value.Dynamic[T]:
		base = val.Base
	default:
		assert.Fail(t, "value %+v does not support stubbing", val)
		return func() {}
	}

	oldGet := base.BoundGetFunc
	base.BoundGetFunc = base.GetFunc(v)

	return func() {
		base.BoundGetFunc = oldGet
	}
}

// func StubValues(v *viper.Viper, values ...any) (reset func()) {
//
// }
