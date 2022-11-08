package registry

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"vitess.io/vitess/go/viperutil/v2/internal/sync"
)

var (
	Static  = viper.New()
	Dynamic = sync.New()

	_ Bindable = (*viper.Viper)(nil)
	_ Bindable = (*sync.Viper)(nil)
)

type Bindable interface {
	BindEnv(vars ...string) error
	BindPFlag(key string, flag *pflag.Flag) error
	RegisterAlias(alias string, key string)
	SetDefault(key string, value any)
}
