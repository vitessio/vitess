/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package config

import "vitess.io/vitess/go/sync2"

//
// MySQL-specific configuration
//

// MySQLClusterConfigurationSettings has the settings for a specific MySQL cluster. It derives its information
// from MySQLConfigurationSettings
type MySQLClusterConfigurationSettings struct {
	MetricQuery          string               // override MySQLConfigurationSettings's, or leave empty to inherit those settings
	CacheMillis          int                  // override MySQLConfigurationSettings's, or leave empty to inherit those settings
	ThrottleThreshold    *sync2.AtomicFloat64 // override MySQLConfigurationSettings's, or leave empty to inherit those settings
	Port                 int                  // Specify if different than 3306 or if different than specified by MySQLConfigurationSettings
	IgnoreHostsCount     int                  // Number of hosts that can be skipped/ignored even on error or on exceeding thresholds
	IgnoreHostsThreshold float64              // Threshold beyond which IgnoreHostsCount applies (default: 0)
	HTTPCheckPort        int                  // Specify if different than specified by MySQLConfigurationSettings. -1 to disable HTTP check
	HTTPCheckPath        string               // Specify if different than specified by MySQLConfigurationSettings
	IgnoreHosts          []string             // override MySQLConfigurationSettings's, or leave empty to inherit those settings
}

// MySQLConfigurationSettings has the general configuration for all MySQL clusters
type MySQLConfigurationSettings struct {
	MetricQuery          string
	CacheMillis          int // optional, if defined then probe result will be cached, and future probes may use cached value
	ThrottleThreshold    *sync2.AtomicFloat64
	Port                 int      // Specify if different than 3306; applies to all clusters
	IgnoreDialTCPErrors  bool     // Skip hosts where a metric cannot be retrieved due to TCP dial errors
	IgnoreHostsCount     int      // Number of hosts that can be skipped/ignored even on error or on exceeding thresholds
	IgnoreHostsThreshold float64  // Threshold beyond which IgnoreHostsCount applies (default: 0)
	HTTPCheckPort        int      // port for HTTP check. -1 to disable.
	HTTPCheckPath        string   // If non-empty, requires HTTPCheckPort
	IgnoreHosts          []string // If non empty, substrings to indicate hosts to be ignored/skipped

	Clusters map[string](*MySQLClusterConfigurationSettings) // cluster name -> cluster config
}
