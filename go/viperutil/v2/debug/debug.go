package debug

import (
	"github.com/spf13/viper"
	"vitess.io/vitess/go/viperutil/v2/internal/registry"
)

func Debug() {
	v := viper.New()
	_ = v.MergeConfigMap(registry.Static.AllSettings())
	_ = v.MergeConfigMap(registry.Dynamic.AllSettings())

	v.Debug()
}
