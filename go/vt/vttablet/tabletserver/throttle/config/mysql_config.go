/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package config

//
// MySQL-specific configuration
//

// MySQLClusterConfigurationSettings has the settings for a specific MySQL cluster. It derives its information
// from MySQLConfigurationSettings
type MySQLClusterConfigurationSettings struct {
	User                 string   // override MySQLConfigurationSettings's, or leave empty to inherit those settings
	Password             string   // override MySQLConfigurationSettings's, or leave empty to inherit those settings
	MetricQuery          string   // override MySQLConfigurationSettings's, or leave empty to inherit those settings
	CacheMillis          int      // override MySQLConfigurationSettings's, or leave empty to inherit those settings
	ThrottleThreshold    float64  // override MySQLConfigurationSettings's, or leave empty to inherit those settings
	Port                 int      // Specify if different than 3306 or if different than specified by MySQLConfigurationSettings
	IgnoreHostsCount     int      // Number of hosts that can be skipped/ignored even on error or on exceeding thresholds
	IgnoreHostsThreshold float64  // Threshold beyond which IgnoreHostsCount applies (default: 0)
	HTTPCheckPort        int      // Specify if different than specified by MySQLConfigurationSettings. -1 to disable HTTP check
	HTTPCheckPath        string   // Specify if different than specified by MySQLConfigurationSettings
	IgnoreHosts          []string // override MySQLConfigurationSettings's, or leave empty to inherit those settings
}

// Hook to implement adjustments after reading each configuration file.
func (settings *MySQLClusterConfigurationSettings) postReadAdjustments() error {
	return nil
}

// MySQLConfigurationSettings has the general configuration for all MySQL clusters
type MySQLConfigurationSettings struct {
	User                 string
	Password             string
	MetricQuery          string
	CacheMillis          int // optional, if defined then probe result will be cached, and future probes may use cached value
	ThrottleThreshold    float64
	Port                 int      // Specify if different than 3306; applies to all clusters
	IgnoreDialTCPErrors  bool     // Skip hosts where a metric cannot be retrieved due to TCP dial errors
	IgnoreHostsCount     int      // Number of hosts that can be skipped/ignored even on error or on exceeding thresholds
	IgnoreHostsThreshold float64  // Threshold beyond which IgnoreHostsCount applies (default: 0)
	HTTPCheckPort        int      // port for HTTP check. -1 to disable.
	HTTPCheckPath        string   // If non-empty, requires HTTPCheckPort
	IgnoreHosts          []string // If non empty, substrings to indicate hosts to be ignored/skipped

	Clusters map[string](*MySQLClusterConfigurationSettings) // cluster name -> cluster config
}

// Hook to implement adjustments after reading each configuration file.
func (settings *MySQLConfigurationSettings) postReadAdjustments() error {
	// Username & password may be given as plaintext in the config file, or can be delivered
	// via environment variables. We accept user & password in the form "${SOME_ENV_VARIABLE}"
	// in which case we get the value from this process' invoking environment.

	for _, clusterSettings := range settings.Clusters {
		if err := clusterSettings.postReadAdjustments(); err != nil {
			return err
		}
		if clusterSettings.User == "" {
			clusterSettings.User = settings.User
		}
		if clusterSettings.Password == "" {
			clusterSettings.Password = settings.Password
		}
		if clusterSettings.MetricQuery == "" {
			clusterSettings.MetricQuery = settings.MetricQuery
		}
		if clusterSettings.CacheMillis == 0 {
			clusterSettings.CacheMillis = settings.CacheMillis
		}
		if clusterSettings.ThrottleThreshold == 0 {
			clusterSettings.ThrottleThreshold = settings.ThrottleThreshold
		}
		if clusterSettings.Port == 0 {
			clusterSettings.Port = settings.Port
		}
		if clusterSettings.IgnoreHostsCount == 0 {
			clusterSettings.IgnoreHostsCount = settings.IgnoreHostsCount
		}
		if clusterSettings.IgnoreHostsThreshold == 0 {
			clusterSettings.IgnoreHostsThreshold = settings.IgnoreHostsThreshold
		}
		if clusterSettings.HTTPCheckPort == 0 {
			clusterSettings.HTTPCheckPort = settings.HTTPCheckPort
		}
		if clusterSettings.HTTPCheckPath == "" {
			clusterSettings.HTTPCheckPath = settings.HTTPCheckPath
		}
		if len(clusterSettings.IgnoreHosts) == 0 {
			clusterSettings.IgnoreHosts = settings.IgnoreHosts
		}
	}
	return nil
}
