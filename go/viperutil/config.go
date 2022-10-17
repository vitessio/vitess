package viperutil

import (
	"os"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/viperutil/viperget"
	"vitess.io/vitess/go/vt/log"
)

var (
	v           = viper.New()
	configPaths = NewValue(
		"config.paths",
		viperget.Path(v),
		WithEnvVars[[]string]("VT_CONFIG_PATH"),
		WithFlags[[]string]("config-path"),
	)

	configType = NewValue(
		"config.type",
		v.GetString,
		WithEnvVars[string]("VT_CONFIG_TYPE"),
		WithFlags[string]("config-type"),
	)
	_configTypeFlagVal = flagutil.NewCaseInsensitiveStringEnum("config-type", configType.Value(), viper.SupportedExts)

	configName = NewValue(
		"config.name",
		v.GetString,
		WithDefault("vtconfig"),
		WithEnvVars[string]("VT_CONFIG_NAME"),
		WithFlags[string]("config-name"),
	)
	configFile = NewValue(
		"config.file",
		v.GetString,
		WithEnvVars[string]("VT_CONFiG_FILE"),
		WithFlags[string]("config-file"),
	)
)

func init() {
	wd, _ := os.Getwd()
	configPaths.value = append(configPaths.value, wd)
}

func RegisterDefaultConfigFlags(fs *pflag.FlagSet) {
	fs.StringSlice("config-path", configPaths.Value(), "") // TODO: usage here
	configPaths.Bind(v, fs)

	fs.Var(_configTypeFlagVal, "config-type", "") // TODO: usage here
	configType.Bind(v, fs)

	fs.String("config-name", configName.Value(), "") // TODO: usage here
	configName.Bind(v, fs)

	fs.String("config-file", configFile.Value(), "") // TODO: usage here
	configFile.Bind(v, fs)
}

type ConfigFileNotFoundHandling int

const (
	IgnoreConfigFileNotFound ConfigFileNotFoundHandling = iota
	WarnOnConfigFileNotFound
	ExitOnConfigFileNotFound
)

type ConfigOptions struct {
	Paths []string
	Type  string
	Name  string
	File  string
}

// ReadInConfig attempts to read a config into the given viper instance.
//
// The ConfigOptions govern the config file searching behavior, with Paths,
// Type, Name, and File behaving as described by the viper documentation [1].
//
// The ConfigFileNotFoundHandling controls whether not finding a config file
// should be ignored, treated as a warning, or exit the program.
//
// [1]: https://pkg.go.dev/github.com/spf13/viper#readme-reading-config-files
func ReadInConfig(v *viper.Viper, opts ConfigOptions, handling ConfigFileNotFoundHandling) error {
	for _, path := range opts.Paths {
		v.AddConfigPath(path)
	}

	if opts.Type != "" {
		v.SetConfigType(opts.Type)
	}

	if opts.Name != "" {
		v.SetConfigName(opts.Name)
	}

	if opts.File != "" {
		v.SetConfigFile(opts.File)
	}

	err := v.ReadInConfig()
	if err != nil {
		if nferr, ok := err.(viper.ConfigFileNotFoundError); ok {
			switch handling {
			case IgnoreConfigFileNotFound:
				return nil
			case WarnOnConfigFileNotFound:
				log.Warningf(nferr.Error())
			case ExitOnConfigFileNotFound:
				log.Exitf(nferr.Error())
			}
		}
	}

	return err
}

// ReadInDefaultConfig reads the default config (governed by the config-* flags
// defined in this package) into the global viper singleton.
//
// It is called by servenv immediately after parsing the command-line flags.
// Modules that manage their own viper instances should merge in this config at
// any point after parsing has occurred (**not** in an init func):
//
//	// Assuming this package has initialized a viper as `v := viper.New()`,
//	// this merges all settings on the global viper into the local viper.
//	v.MergeConfigMap(viper.AllSettings())
//
// This function will log a warning if a config file cannot be found, since
// users may not necessarily wish to use files to manage their configurations.
func ReadInDefaultConfig() error {
	return ReadInConfig(
		viper.GetViper(),
		ConfigOptions{
			Paths: configPaths.Get(),
			Type:  configType.Get(),
			Name:  configName.Get(),
			File:  configFile.Get(),
		},
		WarnOnConfigFileNotFound,
	)
}
