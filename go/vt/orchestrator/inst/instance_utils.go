/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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

package inst

import (
	"regexp"
	"strconv"
	"strings"
)

var (
	DowntimeLostInRecoveryMessage = "lost-in-recovery"
)

// majorVersionsSortedByCount sorts (major) versions:
// - primary sort: by count appearances
// - secondary sort: by version
type majorVersionsSortedByCount struct {
	versionsCount map[string]int
	versions      []string
}

func newMajorVersionsSortedByCount(versionsCount map[string]int) *majorVersionsSortedByCount {
	versions := []string{}
	for v := range versionsCount {
		versions = append(versions, v)
	}
	return &majorVersionsSortedByCount{
		versionsCount: versionsCount,
		versions:      versions,
	}
}

func (majorVersionSorter *majorVersionsSortedByCount) Len() int {
	return len(majorVersionSorter.versions)
}
func (majorVersionSorter *majorVersionsSortedByCount) Swap(i, j int) {
	majorVersionSorter.versions[i], majorVersionSorter.versions[j] = majorVersionSorter.versions[j], majorVersionSorter.versions[i]
}
func (majorVersionSorter *majorVersionsSortedByCount) Less(i, j int) bool {
	if majorVersionSorter.versionsCount[majorVersionSorter.versions[i]] == majorVersionSorter.versionsCount[majorVersionSorter.versions[j]] {
		return majorVersionSorter.versions[i] > majorVersionSorter.versions[j]
	}
	return majorVersionSorter.versionsCount[majorVersionSorter.versions[i]] < majorVersionSorter.versionsCount[majorVersionSorter.versions[j]]
}
func (majorVersionSorter *majorVersionsSortedByCount) First() string {
	return majorVersionSorter.versions[0]
}

// majorVersionsSortedByCount sorts (major) versions:
// - primary sort: by count appearances
// - secondary sort: by version
type binlogFormatSortedByCount struct {
	formatsCount map[string]int
	formats      []string
}

func newBinlogFormatSortedByCount(formatsCount map[string]int) *binlogFormatSortedByCount {
	formats := []string{}
	for v := range formatsCount {
		formats = append(formats, v)
	}
	return &binlogFormatSortedByCount{
		formatsCount: formatsCount,
		formats:      formats,
	}
}

func (binlogFormatSorter *binlogFormatSortedByCount) Len() int {
	return len(binlogFormatSorter.formats)
}
func (binlogFormatSorter *binlogFormatSortedByCount) Swap(i, j int) {
	binlogFormatSorter.formats[i], binlogFormatSorter.formats[j] = binlogFormatSorter.formats[j], binlogFormatSorter.formats[i]
}
func (binlogFormatSorter *binlogFormatSortedByCount) Less(i, j int) bool {
	if binlogFormatSorter.formatsCount[binlogFormatSorter.formats[i]] == binlogFormatSorter.formatsCount[binlogFormatSorter.formats[j]] {
		return IsSmallerBinlogFormat(binlogFormatSorter.formats[j], binlogFormatSorter.formats[i])
	}
	return binlogFormatSorter.formatsCount[binlogFormatSorter.formats[i]] < binlogFormatSorter.formatsCount[binlogFormatSorter.formats[j]]
}
func (binlogFormatSorter *binlogFormatSortedByCount) First() string {
	return binlogFormatSorter.formats[0]
}

// InstancesSorterByExec sorts instances by executed binlog coordinates
type InstancesSorterByExec struct {
	instances  [](*Instance)
	dataCenter string
}

func NewInstancesSorterByExec(instances [](*Instance), dataCenter string) *InstancesSorterByExec {
	return &InstancesSorterByExec{
		instances:  instances,
		dataCenter: dataCenter,
	}
}

func (instancesSorter *InstancesSorterByExec) Len() int { return len(instancesSorter.instances) }
func (instancesSorter *InstancesSorterByExec) Swap(i, j int) {
	instancesSorter.instances[i], instancesSorter.instances[j] = instancesSorter.instances[j], instancesSorter.instances[i]
}
func (instancesSorter *InstancesSorterByExec) Less(i, j int) bool {
	// Returning "true" in this function means [i] is "smaller" than [j],
	// which will lead to [j] be a better candidate for promotion

	// Sh*t happens. We just might get nil while attempting to discover/recover
	if instancesSorter.instances[i] == nil {
		return false
	}
	if instancesSorter.instances[j] == nil {
		return true
	}
	if instancesSorter.instances[i].ExecBinlogCoordinates.Equals(&instancesSorter.instances[j].ExecBinlogCoordinates) {
		// Secondary sorting: "smaller" if not logging replica updates
		if instancesSorter.instances[j].LogReplicationUpdatesEnabled && !instancesSorter.instances[i].LogReplicationUpdatesEnabled {
			return true
		}
		// Next sorting: "smaller" if of higher version (this will be reversed eventually)
		// Idea is that given 5.6 a& 5.7 both of the exact position, we will want to promote
		// the 5.6 on top of 5.7, as the other way around is invalid
		if instancesSorter.instances[j].IsSmallerMajorVersion(instancesSorter.instances[i]) {
			return true
		}
		// Next sorting: "smaller" if of larger binlog-format (this will be reversed eventually)
		// Idea is that given ROW & STATEMENT both of the exact position, we will want to promote
		// the STATEMENT on top of ROW, as the other way around is invalid
		if instancesSorter.instances[j].IsSmallerBinlogFormat(instancesSorter.instances[i]) {
			return true
		}
		// Prefer local datacenter:
		if instancesSorter.instances[j].DataCenter == instancesSorter.dataCenter && instancesSorter.instances[i].DataCenter != instancesSorter.dataCenter {
			return true
		}
		// Prefer if not having errant GTID
		if instancesSorter.instances[j].GtidErrant == "" && instancesSorter.instances[i].GtidErrant != "" {
			return true
		}
		// Prefer candidates:
		if instancesSorter.instances[j].PromotionRule.BetterThan(instancesSorter.instances[i].PromotionRule) {
			return true
		}
	}
	return instancesSorter.instances[i].ExecBinlogCoordinates.SmallerThan(&instancesSorter.instances[j].ExecBinlogCoordinates)
}

// filterInstancesByPattern will filter given array of instances according to regular expression pattern
func filterInstancesByPattern(instances [](*Instance), pattern string) [](*Instance) {
	if pattern == "" {
		return instances
	}
	filtered := [](*Instance){}
	for _, instance := range instances {
		if matched, _ := regexp.MatchString(pattern, instance.Key.DisplayString()); matched {
			filtered = append(filtered, instance)
		}
	}
	return filtered
}

// removeInstance will remove an instance from a list of instances
func RemoveInstance(instances [](*Instance), instanceKey *InstanceKey) [](*Instance) {
	if instanceKey == nil {
		return instances
	}
	for i := len(instances) - 1; i >= 0; i-- {
		if instances[i].Key.Equals(instanceKey) {
			instances = append(instances[:i], instances[i+1:]...)
		}
	}
	return instances
}

// removeBinlogServerInstances will remove all binlog servers from given lsit
func RemoveBinlogServerInstances(instances [](*Instance)) [](*Instance) {
	for i := len(instances) - 1; i >= 0; i-- {
		if instances[i].IsBinlogServer() {
			instances = append(instances[:i], instances[i+1:]...)
		}
	}
	return instances
}

// removeNilInstances
func RemoveNilInstances(instances [](*Instance)) [](*Instance) {
	for i := len(instances) - 1; i >= 0; i-- {
		if instances[i] == nil {
			instances = append(instances[:i], instances[i+1:]...)
		}
	}
	return instances
}

// SemicolonTerminated is a utility function that makes sure a statement is terminated with
// a semicolon, if it isn't already
func SemicolonTerminated(statement string) string {
	statement = strings.TrimSpace(statement)
	statement = strings.TrimRight(statement, ";")
	statement = statement + ";"
	return statement
}

// MajorVersion returns a MySQL major version number (e.g. given "5.5.36" it returns "5.5")
func MajorVersion(version string) []string {
	tokens := strings.Split(version, ".")
	if len(tokens) < 2 {
		return []string{"0", "0"}
	}
	return tokens[:2]
}

// IsSmallerMajorVersion tests two versions against another and returns true if
// the former is a smaller "major" varsion than the latter.
// e.g. 5.5.36 is NOT a smaller major version as comapred to 5.5.40, but IS as compared to 5.6.9
func IsSmallerMajorVersion(version string, otherVersion string) bool {
	thisMajorVersion := MajorVersion(version)
	otherMajorVersion := MajorVersion(otherVersion)
	for i := 0; i < len(thisMajorVersion); i++ {
		thisToken, _ := strconv.Atoi(thisMajorVersion[i])
		otherToken, _ := strconv.Atoi(otherMajorVersion[i])
		if thisToken < otherToken {
			return true
		}
		if thisToken > otherToken {
			return false
		}
	}
	return false
}

// IsSmallerBinlogFormat tests two binlog formats and sees if one is "smaller" than the other.
// "smaller" binlog format means you can replicate from the smaller to the larger.
func IsSmallerBinlogFormat(binlogFormat string, otherBinlogFormat string) bool {
	if binlogFormat == "STATEMENT" {
		return (otherBinlogFormat == "ROW" || otherBinlogFormat == "MIXED")
	}
	if binlogFormat == "MIXED" {
		return otherBinlogFormat == "ROW"
	}
	return false
}

// RegexpMatchPatterns returns true if s matches any of the provided regexpPatterns
func RegexpMatchPatterns(s string, regexpPatterns []string) bool {
	for _, filter := range regexpPatterns {
		if matched, err := regexp.MatchString(filter, s); err == nil && matched {
			return true
		}
	}
	return false
}
