package value

import (
	"errors"
	"fmt"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"vitess.io/vitess/go/viperutil/v2/internal/registry"
	"vitess.io/vitess/go/viperutil/v2/internal/sync"
)

type Registerable interface {
	Key() string
	Registry() registry.Bindable
	Flag(fs *pflag.FlagSet) (*pflag.Flag, error)
}

type Base[T any] struct {
	KeyName    string
	DefaultVal T

	GetFunc      func(v *viper.Viper) func(key string) T
	BoundGetFunc func(key string) T

	Aliases  []string
	FlagName string
	EnvVars  []string
}

func (val *Base[T]) Key() string { return val.KeyName }
func (val *Base[T]) Default() T  { return val.DefaultVal }
func (val *Base[T]) Get() T      { return val.BoundGetFunc(val.Key()) }

var errNoFlagDefined = errors.New("flag not defined")

func (val *Base[T]) Flag(fs *pflag.FlagSet) (*pflag.Flag, error) {
	if val.FlagName == "" {
		return nil, nil
	}

	flag := fs.Lookup(val.FlagName)
	if flag == nil {
		return nil, fmt.Errorf("%w with name %s (for key %s)", errNoFlagDefined, val.FlagName, val.Key()) // TODO: export error
	}

	return flag, nil
}

func (val *Base[T]) Bind(v registry.Bindable) {
	v.SetDefault(val.Key(), val.DefaultVal)

	for _, alias := range val.Aliases {
		v.RegisterAlias(alias, val.Key())
	}

	if len(val.EnvVars) > 0 {
		vars := append([]string{val.Key()}, val.EnvVars...)
		_ = v.BindEnv(vars...)
	}
}

func BindFlags(fs *pflag.FlagSet, values ...Registerable) {
	for _, val := range values {
		flag, err := val.Flag(fs)
		switch {
		case err != nil:
			// TODO: panic
			panic(fmt.Errorf("failed to load flag for %s: %w", val.Key(), err))
		case flag == nil:
			continue
		}

		_ = val.Registry().BindPFlag(val.Key(), flag)
		if flag.Name != val.Key() {
			val.Registry().RegisterAlias(flag.Name, val.Key())
		}
	}
}

// func xxx_DELETE_ME_bindFlag(v registry.Bindable, key string, flagName string) {
// 	registry.AddFlagBinding(func(fs *pflag.FlagSet) {
// 		flag := fs.Lookup(flagName)
// 		if flag == nil {
// 			// TODO: panic
//
// 			// TODO: need a way for API consumers to add flags in an
// 			// OnParse hook, which usually will happen _after_ a call
// 			// to Configure (viperutil.Value doesn't expose this right
// 			// now, which you can see the result of in config.go's init
// 			// func to add the working directory to config-paths).
// 		}
//
// 		_ = v.BindPFlag(key, flag)
//
// 		if flag.Name != key {
// 			v.RegisterAlias(flag.Name, key)
// 		}
// 	})
// }

type Static[T any] struct {
	*Base[T]
}

func NewStatic[T any](base *Base[T]) *Static[T] {
	base.Bind(registry.Static)
	base.BoundGetFunc = base.GetFunc(registry.Static)

	return &Static[T]{
		Base: base,
	}
}

func (val *Static[T]) Registry() registry.Bindable {
	return registry.Static
}

type Dynamic[T any] struct {
	*Base[T]
}

func NewDynamic[T any](base *Base[T]) *Dynamic[T] {
	base.Bind(registry.Dynamic)
	base.BoundGetFunc = sync.AdaptGetter(base.Key(), base.GetFunc, registry.Dynamic)

	return &Dynamic[T]{
		Base: base,
	}
}

func (val *Dynamic[T]) Registry() registry.Bindable {
	return registry.Dynamic
}
