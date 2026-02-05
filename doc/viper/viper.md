# Vitess Viper Guidelines

## What is Viper?

[`viper`][viper] is a configuration-management library for Go programs.
It acts as a registry for configuration values coming from a variety of sources, including:

- Default values.
- Configuration files (JSON, YAML, TOML, and other formats supported), including optionally watching and live-reloading.
- Environment variables.
- Command-line flags, primarily from `pflag.Flag` types.

It is used by a wide variety of Go projects, including [hugo][hugo] and [kops][kops].

## "Normal" Usage

Normally, and if you were to follow the examples on the viper documentation, you "just" load in a config file, maybe bind some flags, and then load values all across your codebase, like so:

```go
// cmd/main.go
package main

import (
    "log"

    "github.com/spf13/pflag"
    "github.com/spf13/viper"

    "example.com/pkg/stuff"
)

func main() {
    pflag.String("name", "", "name to print")
    pflag.Parse()

    viper.AddConfigPath(".")
    viper.AddConfigPath("/var/mypkg")

    if err := viper.ReadInConfig(); err != nil {
        if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
            log.Fatal(err)
        }
    }

    viper.BindPFlags(pflag.CommandLine)
    viper.BindEnv("name", "MY_COOL_ENVVAR")

    stuff.Do()
}

// pkg/stuff/do_stuff.go
package stuff

import (
    "fmt"

    "github.com/spf13/viper"
)

func Do() {
    fmt.Println(viper.GetString("name"))
}
```

While this example is great for getting started with `viper` quickly &mdash; it is very easy, and very fast, to _write_ go from nothing to working code &mdash; it's not likely to scale well for a codebase the size and complexity of Vitess, for several reasons:

1. Everything is globally-accessible.

    Currently, most of the config values in Vitess modules are un-exported (and we un-exported more of these during the `pflag` migration).
    This is a good thing, as it gives each module control over how its configuration values are used, rather than allowing the raw values to leak across package boundaries.

1. [Magical][a_theory_of_modern_go] access and lack of compile-time safety.

    In the above example, `package stuff` just "happens to know" that (1) `package main` binds a value to `viper` with the key `"name"` and (2) `"name"` is going to be bound to a `string` value specifically.

    If `package main` ever changes either of these two facts, `package stuff` is going to break (precisely how it breaks depends on what the changes are), and there's no way to catch this _before_ runtime without writing additional linters. (Note this is strongly-related to point 1 above).

1. Hard to document.

    `viper` does not provide any sort of automatic documentation-generation code (we'll discuss this more later), so we will need to write our own tooling if we want to update our flag documentation with information like "this flag is also settable via this config key and these environment variables".

    If anyone anywhere can just magically try to read a value from the global registry without first declaring (1) that a config value with that key _should_ exist; (2) what flags, aliases, environment variables it reads from; and (3) what type it is, then writing that tooling is going to be vastly more complicated, and possibly impossible to do correctly.

So, we use an approach that requires a bit more verbosity up-front to mitigate these drawbacks to the simpler approach.

## Our Approach

Instead of relying on the global `viper.Viper` singleton, we use a shim layer introduced in `package viperutil` to configure values in a standardized way across the entire Vitess codebase.

This function, `Configure`, then returns a value object, with a `Get` method that returns the actual value from the viper registry.
Packages may then choose to export their config values, or not, as they see fit for their API consumers.

### `Configure` Options

In order to properly configure a value for use, `Configure` needs to know, broadly speaking, three things:

1. The key name being bound.
1. What "things" it should be bound to (i.e. other keys via aliases, environment variables, and flag names), as well as if it has a default value.
1. How to `Get` it out of a viper.

`Configure`, therefore, has the following signature:

```go
func Configure[T any](key string, options Options[T]) Value[T]
```

The first parameter provides the key name (point 1 of our above list); all other information is provided via various `Options` fields, which looks like:

```go
type Options[T any] struct {
    // what "things" to bind to
    Aliases []string
    FlagName string
    EnvVars []string

    // default, if any
    Default T

    // whether it can reload or not (more on this later)
    Dynamic bool

    // how to "get" it from a viper (more on this slightly less later)
    GetFunc func(v *viper.Viper) func(key string) T
}
```

### `Get` funcs

In most cases, module authors will not need to specify a `GetFunc` option, since, if not provided, `viperutil` will do its best to provide a sensible default for the given type `T`.

This requires a fair amount of `reflect`ion code, which we won't go into here, and unfortunately cannot support even all primitive types (notably, array (not slice!!) types).
In these cases, the `GetFuncForType` will panic, allowing the module author to catch this during testing of their package.
They may then provide their own `GetFunc`.

Authors may also want to provide their own `GetFunc` to provide additional logic to load a value even for types supported by `GetFuncForType` (for example, post-processing a string to ensure it is always lowercase).

The full suite of types, both supported and panic-inducing, are documented by way of unit tests in [`go/viperutil/get_func_test.go`](../../go/viperutil/get_func_test.go).

### Dynamic values

Values can be configured to be either static or dynamic.
Static values are loaded once at startup (more precisely, when `viperutil.LoadConfig` is called), and whatever value is loaded at the point will be the result of calling `Get` on that value for the remainder of the process's lifetime.
Dynamic values, conversely, may respond to config changes.

In order for dynamic configs to be truly dynamic, `LoadConfig` must have found a config file (as opposed to pulling values entirely from defaults, flags, and environment variables).
If this is the case, a second viper shim, which backs the dynamic registry, will start a watch on that file, and any changes to that file will be reflected in the `Get` methods of any values configured with `Dynamic: true`.

**An important caveat** is that viper on its own is not threadsafe, meaning that if a config reload is being processed while a value is being accessed, a race condition can occur.
To protect against this, the dynamic registry uses a second shim, [`sync.Viper`](../../go/viperutil/internal/sync/sync.go).
This works by assigning each dynamic value its own `sync.RWMutex`, and locking it for writes whenever a config change is detected. Value `GetFunc`s are then adapted to wrap the underlying get in a `m.RLock(); defer m.RUnlock()` layer.
This means that there's a potential throughput impact of using dynamic values, which module authors should be aware of when deciding to make a given value dynamic.

### A brief aside on flags

In the name of "we will catch as many mistakes as possible in tests" ("mistakes" here referring to typos in flag names, deleting a flag in one place but forgetting to clean up another reference, and so on), `Values` will panic at bind-time if they are configured to bind to a flag name that does not exist.
Then, **as long as every binary is at least invoked** (including just `mycmd --help`) in an end-to-end test, our CI will fail if we ever misconfigure a value in this way.

However, since `Configure` handles the binding of defaults, aliases, and envirnomnent variables, and is usually called in `var` blocks, this binding can actually happen before the module registers its flags via the `servenv.{OnParse,OnParseFor}` hooks.
If we were to also bind any named flags at the point of `Configure`, this would cause panics even if the module later registered a flag with that name.
Therefore, we introduce a separate function, namely `viperutil.BindFlags`, which binds the flags on one or more values, which modules can call _after_ registering their flags, usually in the same `OnParse` hook function.
For example:

```go
package trace

import (
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/viperutil"
	"vitess.io/vitess/go/vt/servenv"
)

var (
	configKey = viperutil.KeyPrefixFunc("trace")

	tracingServer = viperutil.Configure(
		configKey("service"),
		viperutil.Options[string]{
			Default:  "noop",
			FlagName: "tracer",
		},
	)
	enableLogging = viperutil.Configure(
		configKey("enable-logging"),
		viperutil.Options[bool]{
			FlagName: "tracing-enable-logging",
		},
	)
)

func RegisterFlags(fs *pflag.FlagSet) {
	fs.String("tracer", tracingServer.Default(), "tracing service to use")
	fs.Bool("tracing-enable-logging", false, "whether to enable logging in the tracing service")

	viperutil.BindFlags(fs, tracingServer, enableLogging)
}

func init() {
	servenv.OnParse(RegisterFlags)
}
```

## Config Files

`viperutil` provides a few flags that allow binaries to read values from config files in addition to defaults, environment variables and flags.
They are:

- `--config-path`
    - Default: `$(pwd)`
    - EnvVar: `VT_CONFIG_PATH` (parsed exactly like a `$PATH` style shell variable).
    - FlagType: `StringSlice`
    - Behavior: Paths for `ReadInConfig` to search.
- `--config-type`
    - Default: `""`
    - EnvVar: `VT_CONFIG_TYPE`
    - FlagType: `flagutil.StringEnum`
        - Values: everything contained in `viper.SupportedExts`, case-insensitive.
    - Behavior: Force viper to use a particular unmarshalling strategy; required if the config file does not have an extension (by default, viper infers the config type from the file extension).
- `--config-name`
    - Default: `"vtconfig"`
    - EnvVar: `VT_CONFIG_NAME`
    - FlagType: `string`
    - Behavior: Instructs `ReadInConfig` to only look in `ConfigPaths` for files named with this name (with any supported extension, unless `ConfigType` is also set, in which case only with that extension).
- `--config-file`
    - Default: `""`
    - EnvVar: `VT_CONFIG_FILE`
    - FlagType: `string`
    - Behavior: Instructs `ReadInConfig` to search in `ConfigPaths` for explicitly a file with this name. Takes precedence over `ConfigName`.
- `--config-file-not-found-handling`
    - Default: `WarnOnConfigFileNotFound`
    - EnvVar: (none)
    - FlagType: `string` (options: `IgnoreConfigFileNotFound`, `WarnOnConfigFileNotFound`, `ErrorOnConfigFileNotFound`, `ExitOnConfigFileNotFound`)
    - Behavior: If viper is unable to locate a config file (based on the other flags here), then `LoadConfig` will:
        - `Ignore` => do nothing, return no error. Program values will come entirely from defaults, environment variables and flags.
        - `Warn` => log at the WARNING level, but return no error.
        - `Error` => log at the ERROR level and return the error back to the caller (usually `servenv`.)
        - `Exit` => log at the FATAL level, exiting immediately.
- `--config-persistence-min-interval`
    - Default: `1s`
    - EnvVar: `VT_CONFIG_PERSISTENCE_MIN_INTERVAL`
    - FlagType: `time.Duration`
    - Behavior: If viper is watching a config file, in order to synchronize between changes to the file, and changes made in-memory to dynamic values (for example, via vtgate's `/debug/env` endpoint), it will periodically write in-memory changes back to disk, waiting _at least_ this long between writes.
    If the value is 0, each in-memory `Set` is immediately followed by a write to disk.

For more information on how viper searches for config files, see the [documentation][viper_read_in_config_docs].

If viper was able to locate and load a config file, `LoadConfig` will then configure the dynamic registry to set up a watch on that file, enabling all dynamic values to pick up changes to that file for the remainder of the program's execution.
If no config file was used, then dynamic values behave exactly like static values (i.e. the dynamic registry copies in the settings loaded into the static registry, but does not set up a file watch).

### Re-persistence for Dynamic Values

Prior to the introduction of viper in Vitess, certain components (such as `vttablet` or `vtgate`) exposed `/debug/env` HTTP endpoints that permitted the user to modify certain configuration parameters at runtime.

This behavior is still supported, and to maintain consistency between update mechanisms, if:
- A config file was loaded at startup
- A value is configured with the `Dynamic: true` option

then in-memory updates to that value (via `.Set()`) will be written back to disk.
If we skipped this step, then the next time viper reloaded the disk config, the in-memory change would be undone, since viper does a full load rather than something more differential.
Unfortunately, this seems unavoidable.

To migitate against potentially writing to disk "too often" for a given user, the `--config-persistence-min-interval` flag defines the _minimum_ time to wait between writes.
Internally, the system is notified to write "soon" only when a dynamic value is updated.
If the wait period has elapsed between changes, a write happens immediately; otherwise, the system waits out the remainder of the period and persists any changes that happened while it was waiting.
Setting this interval to zero means that writes happen immediately.

## Auto-Documentation

One of the benefits of all values being created through a single function is that we can pretty easily build tooling to generate documentation for the config values available to a given binary.
The exact formatting can be tweaked, obviously, but as an example, something like:

```
{{ .BinaryName }}

{{ range .Values }}
{{ .Key }}:
    - Aliases: {{ join .Aliases ", " }}
    - Environment Variables: {{ join .EnvVars ", " }}
    {{- if hasFlag $.BinaryName .FlagName }}
    - Flag: {{ .FlagName }}
    {{ end -}}
    {{- if hasDefault . }}
    - Default: {{ .Default }}
    {{ end -}}
{{ end }}
```

If/when we migrate other binaries to cobra, we can figure out how to combine this documntation with cobra's doc-generation tooling (which we use for `vtctldclient` and `vtadmin`).

## Debug Endpoint

Any component that parses its flags via one of `servenv`'s parsing methods will get an HTTP endpoint registered at `/debug/config` which displays the full viper configuration for debugging purposes.
It accepts a query parameter to control the format; anything in `viper.SupportedExts` is permitted.

Components that do not use `servenv` to parse their flags may manually register the `(go/viperutil/debug).HandlerFunc` if they wish.

## Caveats and Gotchas

- Config keys are case-insensitive.
`Foo`, `foo`, `fOo`, and `FOO` will all have the same value.
    - **Except** for environment variables, which, when read, are case-sensitive (but the config key they are _bound to_ remains case-insensitive).
      For example, if you have `viper.BindEnv("foo", "VT_FOO")`, then `VT_FOO=1 ./myprogram` will set the value to `1`, but `Vt_FoO=1 ./myprogram will not`.
      The value, though, can still be read _from_ viper as `Foo`, `foo`, `FOO`, and so on.
    
- `Sub` is a split-brain.
  The viper docs discuss using the `Sub` method on a viper to extract a subtree of a config to pass to a submodule.
  This seems like a good idea, but has some fun surprises.
  Each viper maintains its own settings map, and extracting a sub-tree creates a second settings map that is now completely divorced from the parent.
  If you were to `parent.Set(key, value)`, the sub-viper will still have the old value.
  Furthermore, if the parent was watching a config file for changes, the sub-viper is _not_ watching that file.

  For these reasons, we **strongly** discourage use of `v.Sub`.

- The `Unmarshal*` functions rely on `mapstructure` tags, not `json|yaml|...` tags.

- Any config files/paths added _after_ calling `WatchConfig` will not get picked up by that viper, and a viper can only watch a single config file.

[viper]: https://github.com/spf13/viper
[viper_read_in_config_docs]: https://github.com/spf13/viper#reading-config-files

[hugo]: https://github.com/gohugoio/hugo
[kops]: https://github.com/kubernetes/kops

[a_theory_of_modern_go]: https://peter.bourgon.org/blog/2017/06/09/theory-of-modern-go.html
