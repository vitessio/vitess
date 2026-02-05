/*
Copyright 2023 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
Package viperutil provides a utility layer to streamline and standardize
interacting with viper-backed configuration values across vitess components.

The common pattern is for a given module to declare their values by declaring
variables that are the result of calling Configure, for example in package trace:

	package trace

	import "vitess.io/vitess/go/viperutil"

	var (
		modulePrefix = viperutil.KeyPrefixFunc("trace")

		tracingServer = viperutil.Configure(
			modulePrefix("service"),
			viperutil.Options[string]{
				Default: "noop",
				FlagName: "tracer",
			}
		)
		enableLogging = viperutil.Configure(
			modulePrefix("enable-logging"),
			viperutil.Options[bool]{
				FlagName: "tracing-enable-logging",
			}
		)
	)

Then, in an OnParseFor or OnParse hook, declare any flags, and bind viper values
to those flags, as appropriate:

	package trace

	import (
		"github.com/spf13/pflag"

		"vitess.io/vitess/go/viperutil"
		"vitess.io/vitess/go/vt/servenv"
	)

	func init() {
		servenv.OnParse(func(fs *pflag.FlagSet) {
			fs.String("tracer", tracingServer.Default(), "<usage for flag goes here>")
			fs.Bool("tracing-enable-logging", enableLogging.Default(), "<usage for flag goes here>")

			viperutil.BindFlags(fs, tracingServer, enableLogging)
		})
	}

Finally, after a call to `viperutil.LoadConfig` (which is done as a part of
`servenv.ParseFlags`), values may be accessed by calling their `.Get()` methods.

For more details, refer to the package documentation, as well as the documents
in doc/viper/.
*/
package viperutil

import (
	"strings"

	"github.com/spf13/viper"

	"vitess.io/vitess/go/viperutil/internal/value"
)

// Options represents the various options used to control how Values are
// configured by viperutil.
type Options[T any] struct {
	// Aliases, if set, configures the Value to be accessible via additional
	// keys.
	//
	// This is useful for deprecating old names gracefully while maintaining
	// backwards-compatibility.
	Aliases []string
	// FlagName, if set, allows a value to be configured to also check the
	// named flag for its final config value. If depending on a flag, BindFlags
	// must be called on the Value returned from Configure. In most cases,
	// modules will do this in the same OnParse hook that defines their flags.
	//
	// Note that if the set FlagName does not match the Value's key, Configure
	// will automatically register an alias to allow both names to be used as
	// the key, which is necessary for the flag value to be discoverable by
	// viper for the Value's actual key.
	FlagName string
	// EnvVars, if set, configures the Value to also check the given environment
	// variables for its final config value.
	//
	// Note that unlike keys and aliases, environment variable names are
	// case-sensitive.
	EnvVars []string
	// Default is the default value that will be set for the key. If not
	// explicitly set during a call to Configure, the default value will be the
	// zero value for the type T. This means if T is a pointer type, the default
	// will be nil, not the zeroed out struct.
	Default T

	// Dynamic, if set, configures a value to be backed by the dynamic registry.
	// If a config file is used (via LoadConfig), that file will be watched for
	// changes, and dynamic Values will reflect changes via their Get() methods
	// (whereas static values will only ever return the value loaded initially).
	Dynamic bool

	// GetFunc is the function used to get this value out of a viper.
	//
	// If omitted, GetFuncForType will attempt to provide a useful default for
	// the given type T. For primitive types, this should be sufficient. For
	// more fine-grained control over value retrieval and unmarshalling, callers
	// should provide their own function.
	//
	// See GetFuncForType for further details.
	GetFunc func(v *viper.Viper) func(key string) T
}

// Configure configures a viper-backed value associated with the given key to
// either the static or dynamic internal registries, returning the resulting
// Value. This value is partially ready for use (it will be able to get values
// from environment variables and defaults), but file-based configs will not
// available until servenv calls LoadConfig, and flag-based configs will not be
// available until a combination of BindFlags and pflag.Parse have been called,
// usually by servenv.
//
// Exact behavior of how the key is bound to the registries depend on the
// Options provided,
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

// KeyPrefixFunc is a helper function to allow modules to extract a common key
// prefix used by that module to avoid repitition (and typos, missed updates,
// and so on).
//
// For example, package go/vt/vttablet/schema may want to do:
//
//	moduleKey := viperutil.KeyPrefixFunc("vttablet.schema")
//	watch := viperutil.Configure(moduleKey("watch_interval"), ...) // => "vttablet.schema.watch_interval"
//	// ... and so on
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
