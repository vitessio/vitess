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

// This codebase originates from https://github.com/github/freno, See https://github.com/github/freno/blob/master/LICENSE
/*
	MIT License

	Copyright (c) 2017 GitHub

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
*/

package config

import (
	"sync/atomic"
)

//
// MySQL-specific configuration
//

// MySQLClusterConfigurationSettings has the settings for a specific MySQL cluster. It derives its information
// from MySQLConfigurationSettings
type MySQLClusterConfigurationSettings struct {
	MetricQuery          string         // override MySQLConfigurationSettings's, or leave empty to inherit those settings
	CacheMillis          int            // override MySQLConfigurationSettings's, or leave empty to inherit those settings
	ThrottleThreshold    *atomic.Uint64 // override MySQLConfigurationSettings's, or leave empty to inherit those settings
	Port                 int            // Specify if different than 3306 or if different than specified by MySQLConfigurationSettings
	IgnoreHostsCount     int            // Number of hosts that can be skipped/ignored even on error or on exceeding thresholds
	IgnoreHostsThreshold float64        // Threshold beyond which IgnoreHostsCount applies (default: 0)
	HTTPCheckPort        int            // Specify if different than specified by MySQLConfigurationSettings. -1 to disable HTTP check
	HTTPCheckPath        string         // Specify if different than specified by MySQLConfigurationSettings
	IgnoreHosts          []string       // override MySQLConfigurationSettings's, or leave empty to inherit those settings
}

// MySQLConfigurationSettings has the general configuration for all MySQL clusters
type MySQLConfigurationSettings struct {
	MetricQuery          string
	CacheMillis          int // optional, if defined then probe result will be cached, and future probes may use cached value
	ThrottleThreshold    *atomic.Uint64
	Port                 int      // Specify if different than 3306; applies to all clusters
	IgnoreDialTCPErrors  bool     // Skip hosts where a metric cannot be retrieved due to TCP dial errors
	IgnoreHostsCount     int      // Number of hosts that can be skipped/ignored even on error or on exceeding thresholds
	IgnoreHostsThreshold float64  // Threshold beyond which IgnoreHostsCount applies (default: 0)
	HTTPCheckPort        int      // port for HTTP check. -1 to disable.
	HTTPCheckPath        string   // If non-empty, requires HTTPCheckPort
	IgnoreHosts          []string // If non empty, substrings to indicate hosts to be ignored/skipped

	Clusters map[string](*MySQLClusterConfigurationSettings) // cluster name -> cluster config
}
