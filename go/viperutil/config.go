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

package viperutil

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"vitess.io/vitess/go/viperutil/funcs"
	"vitess.io/vitess/go/viperutil/internal/log"
	"vitess.io/vitess/go/viperutil/internal/registry"
	"vitess.io/vitess/go/viperutil/internal/value"
)

var (
	configPaths = Configure(
		"config.paths",
		Options[[]string]{
			GetFunc:  funcs.GetPath,
			EnvVars:  []string{"VT_CONFIG_PATH"},
			FlagName: "config-path",
		},
	)
	configType = Configure(
		"config.type",
		Options[string]{
			EnvVars:  []string{"VT_CONFIG_TYPE"},
			FlagName: "config-type",
		},
	)
	configName = Configure(
		"config.name",
		Options[string]{
			Default:  "vtconfig",
			EnvVars:  []string{"VT_CONFIG_NAME"},
			FlagName: "config-name",
		},
	)
	configFile = Configure(
		"config.file",
		Options[string]{
			EnvVars:  []string{"VT_CONFIG_FILE"},
			FlagName: "config-file",
		},
	)
	configFileNotFoundHandling = Configure(
		"config.notfound.handling",
		Options[ConfigFileNotFoundHandling]{
			Default: WarnOnConfigFileNotFound,
			GetFunc: getHandlingValue,
		},
	)
	configPersistenceMinInterval = Configure(
		"config.persistence.min_interval",
		Options[time.Duration]{
			Default:  time.Second,
			EnvVars:  []string{"VT_CONFIG_PERSISTENCE_MIN_INTERVAL"},
			FlagName: "config-persistence-min-interval",
		},
	)
)

func init() {
	wd, err := os.Getwd()
	if err != nil {
		log.WARN("failed to get working directory (err=%v), not appending to default config-paths", err)
		return
	}

	configPaths.(*value.Static[[]string]).DefaultVal = []string{wd}
	// Need to re-trigger the SetDefault call done during Configure.
	registry.Static.SetDefault(configPaths.Key(), configPaths.Default())
}

// RegisterFlags installs the flags that control viper config-loading behavior.
// It is exported to be called by servenv before parsing flags for all binaries.
//
// It cannot be registered here via servenv.OnParse since this causes an import
// cycle.
func RegisterFlags(fs *pflag.FlagSet) {
	fs.StringSlice("config-path", configPaths.Default(), "Paths to search for config files in.")
	fs.String("config-type", configType.Default(), "Config file type (omit to infer config type from file extension).")
	fs.String("config-name", configName.Default(), "Name of the config file (without extension) to search for.")
	fs.String("config-file", configFile.Default(), "Full path of the config file (with extension) to use. If set, --config-path, --config-type, and --config-name are ignored.")
	fs.Duration("config-persistence-min-interval", configPersistenceMinInterval.Default(), "minimum interval between persisting dynamic config changes back to disk (if no change has occurred, nothing is done).")

	var h = configFileNotFoundHandling.Default()
	fs.Var(&h, "config-file-not-found-handling", fmt.Sprintf("Behavior when a config file is not found. (Options: %s)", strings.Join(handlingNames, ", ")))

	BindFlags(fs, configPaths, configType, configName, configFile)
}

// LoadConfig attempts to find, and then load, a config file for viper-backed
// config values to use.
//
// Config searching follows the behavior used by viper [1], namely:
//   - --config-file (full path, including extension) if set will be used to the
//     exclusion of all other flags.
//   - --config-type is required if the config file does not have one of viper's
//     supported extensions (.yaml, .yml, .json, and so on)
//
// An additional --config-file-not-found-handling flag controls how to treat the
// situation where viper cannot find any config files in any of the provided
// paths (for ex, users may want to exit immediately if a config file that
// should exist doesn't for some reason, or may wish to operate with flags and
// environment variables alone, and not use config files at all).
//
// If a config file is successfully loaded, then the dynamic registry will also
// start watching that file for changes. In addition, in-memory changes to the
// config (for example, from a vtgate or vttablet's debugenv) will be persisted
// back to disk, with writes occuring no more frequently than the
// --config-persistence-min-interval flag.
//
// A cancel function is returned to stop the re-persistence background thread,
// if one was started.
//
// [1]: https://github.com/spf13/viper#reading-config-files.
func LoadConfig() (context.CancelFunc, error) {
	var err error
	switch file := configFile.Get(); file {
	case "":
		if name := configName.Get(); name != "" {
			registry.Static.SetConfigName(name)

			for _, path := range configPaths.Get() {
				registry.Static.AddConfigPath(path)
			}

			if cfgType := configType.Get(); cfgType != "" {
				registry.Static.SetConfigType(cfgType)
			}

			err = registry.Static.ReadInConfig()
		}
	default:
		registry.Static.SetConfigFile(file)
		err = registry.Static.ReadInConfig()
	}

	if err != nil {
		if nferr, ok := err.(viper.ConfigFileNotFoundError); ok {
			msg := "Failed to read in config %s: %s"
			switch configFileNotFoundHandling.Get() {
			case WarnOnConfigFileNotFound:
				log.WARN(msg, registry.Static.ConfigFileUsed(), nferr.Error())
				fallthrough // after warning, ignore the error
			case IgnoreConfigFileNotFound:
				err = nil
			case ErrorOnConfigFileNotFound:
				log.ERROR(msg, registry.Static.ConfigFileUsed(), nferr.Error())
			case ExitOnConfigFileNotFound:
				log.CRITICAL(msg, registry.Static.ConfigFileUsed(), nferr.Error())
			}
		}
	}

	if err != nil {
		return nil, err
	}

	return registry.Dynamic.Watch(context.Background(), registry.Static, configPersistenceMinInterval.Get())
}

// NotifyConfigReload adds a subscription that the dynamic registry will attempt
// to notify on config changes. The notification fires after the updated config
// has been loaded from disk into the live config.
//
// Analogous to signal.Notify, notifications are sent non-blocking, so users
// should account for this when writing code to consume from the channel.
//
// This function must be called prior to LoadConfig; it will panic if called
// after the dynamic registry has started watching the loaded config.
func NotifyConfigReload(ch chan<- struct{}) {
	registry.Dynamic.Notify(ch)
}

// ConfigFileNotFoundHandling is an enum to control how LoadConfig treats errors
// of type viper.ConfigFileNotFoundError when loading a config.
type ConfigFileNotFoundHandling int

const (
	// IgnoreConfigFileNotFound causes LoadConfig to completely ignore a
	// ConfigFileNotFoundError (i.e. not even logging it).
	IgnoreConfigFileNotFound ConfigFileNotFoundHandling = iota
	// WarnOnConfigFileNotFound causes LoadConfig to log a warning with details
	// about the failed config load, but otherwise proceeds with the given
	// process, which will get config values entirely from defaults,
	// envirnoment variables, and flags.
	WarnOnConfigFileNotFound
	// ErrorOnConfigFileNotFound causes LoadConfig to return the
	// ConfigFileNotFoundError after logging an error.
	ErrorOnConfigFileNotFound
	// ExitOnConfigFileNotFound causes LoadConfig to log.Fatal on a
	// ConfigFileNotFoundError.
	ExitOnConfigFileNotFound
)

var (
	handlingNames         []string
	handlingNamesToValues = map[string]int{
		"ignore": int(IgnoreConfigFileNotFound),
		"warn":   int(WarnOnConfigFileNotFound),
		"error":  int(ErrorOnConfigFileNotFound),
		"exit":   int(ExitOnConfigFileNotFound),
	}
	handlingValuesToNames map[int]string
)

func getHandlingValue(v *viper.Viper) func(key string) ConfigFileNotFoundHandling {
	return func(key string) (h ConfigFileNotFoundHandling) {
		if err := v.UnmarshalKey(key, &h, viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(decodeHandlingValue))); err != nil {
			h = IgnoreConfigFileNotFound
			log.WARN("failed to unmarshal %s: %s; defaulting to %s", err.Error(), key, err.Error(), h.String())
		}

		return h
	}
}

func decodeHandlingValue(from, to reflect.Type, data any) (any, error) {
	var h ConfigFileNotFoundHandling
	if to != reflect.TypeOf(h) {
		return data, nil
	}

	switch {
	case from == reflect.TypeOf(h):
		return data.(ConfigFileNotFoundHandling), nil
	case from.Kind() == reflect.Int:
		return ConfigFileNotFoundHandling(data.(int)), nil
	case from.Kind() == reflect.String:
		if err := h.Set(data.(string)); err != nil {
			return h, err
		}

		return h, nil
	}

	return data, fmt.Errorf("invalid value for ConfigHandlingType: %v", data)
}

func init() {
	handlingNames = make([]string, 0, len(handlingNamesToValues))
	handlingValuesToNames = make(map[int]string, len(handlingNamesToValues))

	for name, val := range handlingNamesToValues {
		handlingValuesToNames[val] = name
		handlingNames = append(handlingNames, name)
	}

	sort.Slice(handlingNames, func(i, j int) bool {
		return handlingNames[i] < handlingNames[j]
	})
}

func (h *ConfigFileNotFoundHandling) Set(arg string) error {
	larg := strings.ToLower(arg)
	if v, ok := handlingNamesToValues[larg]; ok {
		*h = ConfigFileNotFoundHandling(v)
		return nil
	}

	return fmt.Errorf("unknown handling name %s", arg)
}

func (h *ConfigFileNotFoundHandling) String() string {
	if name, ok := handlingValuesToNames[int(*h)]; ok {
		return name
	}

	return "<UNKNOWN>"
}

func (h *ConfigFileNotFoundHandling) Type() string { return "ConfigFileNotFoundHandling" }
