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

## Common Usage

Broadly speaking, there's two approaches for Vitess to using viper.

### Approach 1: Everything in the global registry.

In this approach, we simply:

```go
import (
    "github.com/spf13/pflag"
    "github.com/spf13/viper"

    // See the section on viperutil for details on this package.
    "vitess.io/vitess/go/viperutil"
    "vitess.io/vitess/go/vt/servenv"
)

var myValue = viperutil.NewValue(
    "mykey", viper.GetString,
    viperutil.WithFlags[string]("myflagname"),
    viperutil.WithDefault("defaultvalue"),
)

func init() {
    servenv.OnParseFor("mycmd", func(fs *pflag.FlagSet) {
        fs.String("myflagname", myValue.Value(), "help text for myflagname")
        myValue.Bind(
            /* nil here means use the global viper */ nil,
            fs,
        )
    })
}

func DoAThingWithMyValue() {
    fmt.Println("mykey=", myValue.Get())
}
```

Pros:
- Easy to read and write.
- Easy to debug (we can provide a flag to all binaries that results in calling `viper.Debug()`, which would dump out every setting from every vitess module).

Cons:
- Requires us to be disciplined about cross-module boundaries.
    - Anyone anywhere can then do `viper.GetString("mykey")` and retrieve the value.
    - Even more scarily, anyone anywhere can _override_ the value via `viper.Set("mykey", 2)` (notice I've even changed the type here).
        - Even more _more_ scarily, see below about threadsafety for how dangerous this can be.

### Approach 2: Package-local vipers

Instead of putting everything in the global registry, each package can declare a local `viper` instance and put its configuration there. Instead of the above example, this would look like:

```go
import (
    "github.com/spf13/pflag"
    "github.com/spf13/viper"

    "vitess.io/vitess/go/viperutil"
    "vitess.io/vitess/go/vt/servenv"
)

var (
    v = viper.New()
    myValue = viperutil.NewValue(
    "mykey", v.GetString,
        viperutil.WithFlags[string]("myflagname"),
        viperutil.WithDefault("defaultvalue"),
    ),
)

func init() {
    servenv.OnParseFor("mycmd", func(fs *pflag.FlagSet) {
        fs.String("myflagname", myValue.Value(), "help text for myflagname")
        myValue.Bind(
            /* bind to our package-local viper instance */ v,
            fs,
        )
    })
}

func DoAThingWithMyValue() {
    fmt.Println("mykey=", myValue.Get())
}
```

Pros:
- Maintains package-private nature of configuration values we currently have.

Cons:
- No easy "show me all the debug settings everywhere". We would have to have each package expose a function to call, or come up with some other solution.
- Hard to read and write.
    - "Wait, what is this `v` thing and where did it come from?

### Approach 2.2: Readability changes via import alias.

To address the readability issue in Approach 2, we could alias the viper import to let us write "normal" looking viper code:

```go
import (
    _viper "github.com/spf13/viper"

    "vitess.io/vitess/go/viperutil"
)

var (
    viper = _viper.New()
    myValue = viperutil.NewValue(
    "mykey", viper.GetString,
        viperutil.WithFlags[string]("myflagname"),
        viperutil.WithDefault("defaultvalue"),
    ),
)
```

The problem _here_ (in addition to it being admittedly a little sneaky), is that if you were to open a new file in the same package, your IDE would very likely pick up the "viper" string and simply import the package without the alias (which also has a `GetString` function), and now you have two files in the same package, one working with the local variable, and the other working with the global registry, and it would be _verrrrry_ tricky to notice in code review. To address that we could write a simple linter to verify that (with the exception of explicitly allow-listed modules), all Vitess modules only ever import `viper` as the aliased version.

## `go/viperutil`

## `go/viperutil/viperget`

## Caveats and Gotchas

- [ ] case-(in)sensitivity.
- [ ] Threadsafety.
- [ ] `Sub` is split-brain
- [ ] `Unmarshal*` functions rely on `mapstructure` tags, not `json|yaml|...` tags.
- [ ] Any config files/paths added _after_ calling `WatchConfig` will not get picked up.

[viper]: https://github.com/spf13/viper
[hugo]: https://github.com/gohugoio/hugo
[kops]: https://github.com/kubernetes/kops
