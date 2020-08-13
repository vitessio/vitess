/*
   Copyright 2014 Outbrain Inc.

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

// Process presents a MySQL executing thread (as observed by PROCESSLIST)
type Process struct {
	InstanceHostname string
	InstancePort     int
	Id               int64
	User             string
	Host             string
	Db               string
	Command          string
	Time             int64
	State            string
	Info             string
	StartedAt        string
}
