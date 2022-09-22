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

package config

// CLIFlags stores some command line flags that are globally available in the process' lifetime
type CLIFlags struct {
	Noop                       *bool
	SkipUnresolve              *bool
	SkipUnresolveCheck         *bool
	BinlogFile                 *string
	GrabElection               *bool
	Version                    *bool
	Statement                  *string
	PromotionRule              *string
	ConfiguredVersion          string
	SkipContinuousRegistration *bool
	EnableDatabaseUpdate       *bool
	IgnoreRaftSetup            *bool
	Tag                        *string
}

var RuntimeCLIFlags CLIFlags
