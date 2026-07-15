/*
Copyright 2019 The Vitess Authors.

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

/*
 Detect server flavors and capabilities
*/

package mysqlctl

type ServerVersion struct {
	Major, Minor, Patch int
}

func (v *ServerVersion) atLeast(compare ServerVersion) bool {
	if v.Major > compare.Major {
		return true
	}
	if v.Major == compare.Major && v.Minor > compare.Minor {
		return true
	}
	if v.Major == compare.Major && v.Minor == compare.Minor && v.Patch >= compare.Patch {
		return true
	}
	return false
}

func (v *ServerVersion) IsSameRelease(compare ServerVersion) bool {
	return v.Major == compare.Major && v.Minor == compare.Minor
}

// mysql80BugfixOnlyPatch is the first MySQL 8.0 patch release that is
// bugfix-only. Starting with 8.0.34 the 8.0 series receives no new features
// (feature work moved to the 8.1+ Innovation releases), so two 8.0 servers both
// at or above this patch have no format/feature delta between them. Feature
// additions that can break newer-source-to-older-replica replication within the
// 8.0 series (e.g. binary log transaction compression added in 8.0.20) all
// predate it. See https://dev.mysql.com/doc/refman/8.0/en/replication-compatibility.html
const mysql80BugfixOnlyPatch = 34

// CompareForReplication compares two server versions for the purpose of
// replication-compatibility ordering, returning -1 if v is "lower" than compare,
// +1 if higher, and 0 if they are equivalent for replication.
//
// Versions are ordered by major.minor. The patch component is normally ignored,
// because patch releases are bugfix-only and do not change replication
// compatibility — with one exception: within the MySQL 8.0 series, feature
// additions before 8.0.34 (the first bugfix-only 8.0 patch) can make a newer
// patch incompatible as a source to an older-patch replica. So when both
// versions are in the 8.0 series and the lower of the two patches is below
// 8.0.34, the patch is significant and is compared as well.
//
// Callers must only compare versions within the same flavor family; comparing
// across families (e.g. MariaDB vs MySQL) is not meaningful.
func (v *ServerVersion) CompareForReplication(compare ServerVersion) int {
	if v.Major != compare.Major {
		return sign(v.Major - compare.Major)
	}
	if v.Minor != compare.Minor {
		return sign(v.Minor - compare.Minor)
	}
	// Same major.minor. Only within the pre-bugfix-only 8.0 series does the patch
	// affect replication compatibility.
	if v.Major == 8 && v.Minor == 0 && min(v.Patch, compare.Patch) < mysql80BugfixOnlyPatch {
		return sign(v.Patch - compare.Patch)
	}
	return 0
}

func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}
