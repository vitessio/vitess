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
	vtorcdatapb "vitess.io/vitess/go/vt/proto/vtorcdata"
	"vitess.io/vitess/go/vt/vtorc/config"
)

// PeerAnalysisMap indicates the number of peers agreeing on an analysis.
// Key of this map is a InstanceAnalysis.String()
type PeerAnalysisMap map[string]int

type ReplicationAnalysisHints struct {
	AuditAnalysis bool
}

// ValidSecondsFromSeenToLastAttemptedCheck returns the maximum allowed elapsed time
// between last_attempted_check to last_checked before we consider the instance as invalid.
func ValidSecondsFromSeenToLastAttemptedCheck() uint {
	return config.GetInstancePollSeconds()
}

// AnalysisTypeStringToProto converts an analysis type string to a vtorcdatapb.AnalysisType.
func AnalysisTypeStringToProto(analysisType string) vtorcdatapb.AnalysisType {
	if i, found := vtorcdatapb.AnalysisType_value[analysisType]; found {
		return vtorcdatapb.AnalysisType(i)
	}
	return vtorcdatapb.AnalysisType_NoProblem
}

// AnalysisTypeProtoToString converts an vtorcdatapb.AnalysisType to a string.
func AnalysisTypeProtoToString(analysisType vtorcdatapb.AnalysisType) string {
	if str, found := vtorcdatapb.AnalysisType_name[int32(analysisType)]; found {
		return str
	}
	return vtorcdatapb.AnalysisType_name[0]
}

// gtidModeToProto converts a gtid_mode string to a vtorcdatapb.GTIDMode.
func gtidModeToProto(gtidMode string) vtorcdatapb.GTIDMode {
	if i, found := vtorcdatapb.GTIDMode_value[gtidMode]; found {
		return vtorcdatapb.GTIDMode(i)
	}
	return vtorcdatapb.GTIDMode_OFF
}
