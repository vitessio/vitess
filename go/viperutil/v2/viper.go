package viperutil

import (
	"strings"

	"github.com/spf13/viper"

	"vitess.io/vitess/go/viperutil/v2/internal/value"
)

type Options[T any] struct {
	Aliases  []string
	FlagName string
	EnvVars  []string
	Default  T

	Dynamic bool

	GetFunc func(v *viper.Viper) func(key string) T
}

func Configure[T any](key string, opts Options[T]) (v Value[T]) {
	getfunc := opts.GetFunc
	if getfunc == nil {
		getfunc = GetFuncForType[T]()
	}

	base := &value.Base[T]{
		KeyName:    key,
		DefaultVal: opts.Default,
		GetFunc:    getfunc,
		Aliases:    opts.Aliases,
		FlagName:   opts.FlagName,
		EnvVars:    opts.EnvVars,
	}

	switch {
	case opts.Dynamic:
		v = value.NewDynamic(base)
	default:
		v = value.NewStatic(base)
	}

	return v
}

func KeyPrefixFunc(prefix string) func(subkey string) (fullkey string) {
	var keyParts []string
	if prefix != "" {
		keyParts = append(keyParts, prefix)
	}

	return func(subkey string) (fullkey string) {
		tmp := keyParts
		if subkey != "" {
			tmp = append(tmp, subkey)
		}

		return strings.Join(tmp, ".")
	}
}
