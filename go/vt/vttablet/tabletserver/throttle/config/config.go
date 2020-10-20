/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package config

// Instance is the one configuration for the throttler
var Instance = &ConfigurationSettings{}

// Settings returns the settings of the global instance of Configuration
func Settings() *ConfigurationSettings {
	return Instance
}

// ConfigurationSettings models a set of configurable values, that can be
// provided by the user via one or several JSON formatted files.
//
// Some of the settings have reasonable default values, and some other
// (like database credentials) are strictly expected from user.
type ConfigurationSettings struct {
	ListenPort      int
	DataCenter      string
	Environment     string
	Domain          string
	EnableProfiling bool // enable pprof profiling http api
	Stores          StoresSettings
}

// PostReadAdjustments validates and fixes config
func (settings *ConfigurationSettings) PostReadAdjustments() error {
	if err := settings.Stores.postReadAdjustments(); err != nil {
		return err
	}
	return nil
}
