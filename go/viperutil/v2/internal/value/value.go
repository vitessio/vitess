package value

import (
	"errors"
	"fmt"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"vitess.io/vitess/go/viperutil/v2/internal/registry"
	"vitess.io/vitess/go/viperutil/v2/internal/sync"
)

// Registerable is the subset of the interface exposed by Values (which is
// declared in the public viperutil package).
//
// We need a separate interface type because Go generics do not let you define
// a function that takes Value[T] for many, different T's, which we want to do
// for BindFlags.
type Registerable interface {
	Key() string
	Registry() registry.Bindable
	Flag(fs *pflag.FlagSet) (*pflag.Flag, error)
}

// Base is the base functionality shared by Static and Dynamic values. It
// implements viperutil.Value.
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

// ErrNoFlagDefined is returned when a Value has a FlagName set, but the given
// FlagSet does not define a flag with that name.
var ErrNoFlagDefined = errors.New("flag not defined")

func (val *Base[T]) Flag(fs *pflag.FlagSet) (*pflag.Flag, error) {
	if val.FlagName == "" {
		return nil, nil
	}

	flag := fs.Lookup(val.FlagName)
	if flag == nil {
		return nil, fmt.Errorf("%w with name %s (for key %s)", ErrNoFlagDefined, val.FlagName, val.Key())
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
