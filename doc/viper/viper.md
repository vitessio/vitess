# Vitess Viper Guidelines

### Preface

## What is Viper?

[`viper`][viper] is a configuration-management library for Go programs.
It acts as a registry for configuration values coming from a variety of sources, including:

- Default values.
- Configuration files (JSON, YAML, TOML, and other formats supported), including optionally watching and live-reloading.
- Environment variables.
- Command-line flags, primarily from `pflag.Flag` types.

It is used by a wide variety of Go projects, including [hugo][hugo] and [the kubernetes operator][kops].

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
Static values are loaded once at startup (more precisely, when `viperutil.LoadConfig` is called), and whatever value is loaded at the point will be the result of calling `Get` on that value for the remainder of the processes lifetime.
Dynamic values, conversely, may respond to config changes.

In order for dynamic configs to be truly dynamic, `LoadConfig` must have found a config file (as opposed to pulling values entirely from defaults, flags, and environment variables).
If this is the case, a second viper shim, which backs the dynamic registry, will start a watch on that file, and any changes to that file will be reflected in the `Get` methods of any values configured with `Dynamic: true`.

**An important caveat** is that viper on its own is not threadsafe, meaning that if a config reload is being processed while a value is being accessed, a race condition can occur.
To protect against this, the dynamic registry uses a second shim, [`sync.Viper`](../../go/viperutil/internal/sync/sync.go).
This works by assigning each dynamic value its own `sync.RWMutex`, and locking it for writes whenever a config change is detected. Value `GetFunc`s are then adapted to wrap the underlying get in a `m.RLock(); defer m.RUnlock()` layer.
This means that there's a potential throughput impact of using dynamic values, which module authors should be aware of when deciding to make a given value dynamic.

### A brief aside on flags

## Auto-Documentation

<!-- old stuff starts here -->

## Config File(s)

All vitess components will support taking a single, static (i.e. not "watched" via `WatchConfig`) configuration file.

The file will be loaded via `ReadInConfig`.
We will log a warning if no file was found, but will not abort (see [docs][viper_read_in_config_docs]).

Flags for all binaries:
- `--config-path`
    - Default: `$(pwd)`
    - EnvVar: `VT_CONFIG_PATH` (parsed exactly like a `$PATH` style shell variable).
    - FlagType: `StringSlice`
    - Behavior: Paths for `ReadInConfig` to search.
- `--config-type` (default: "")
    - Default: `""`
    - EnvVar: `VT_CONFIG_TYPE`
    - FlagType: `flagutil.StringEnum`
        - Values: everything contained in `viper.SupportedExts`, case-insensitive.
    - Behavior: Force viper to use a particular unmarshalling strategy; required if the config file does not have an extension (by default, viper infers the config type from the file extension).
- `--config-name` (default: "vtconfig")
    - Default: `"vtconfig"`
    - EnvVar: `VT_CONFIG_NAME`
    - FlagType: `string`
    - Behavior: Instructs `ReadInConfig` to only look in `ConfigPaths` for files named with this name (with any supported extension, unless `ConfigType` is also set, in which case only with that extension).
- `--config-file`
    - Default: `""`
    - EnvVar: `VT_CONFIG_FILE`
    - FlagType: `string`
    - Behavior: Instructs `ReadInConfig` to search in `ConfigPaths` for explicitly a file with this name. Takes precedence over `ConfigName`.

## Caveats and Gotchas

- [ ] case-(in)sensitivity.
- [ ] `Sub` is split-brain
- [ ] `Unmarshal*` functions rely on `mapstructure` tags, not `json|yaml|...` tags.
- [ ] Any config files/paths added _after_ calling `WatchConfig` will not get picked up.

[viper]: https://github.com/spf13/viper
[viper_read_in_config_docs]: https://github.com/spf13/viper#reading-config-files

[hugo]: https://github.com/gohugoio/hugo
[kops]: https://github.com/kubernetes/kops

[a_theory_of_modern_go]: https://peter.bourgon.org/blog/2017/06/09/theory-of-modern-go.html
