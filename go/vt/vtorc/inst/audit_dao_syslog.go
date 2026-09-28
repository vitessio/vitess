//go:build !windows

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

import "log/syslog"

// syslogWriter is optional, and defaults to nil (disabled)
var syslogWriter *syslog.Writer

// EnableAuditSyslog enables, if possible, writes to syslog. These will execute _in addition_ to normal logging
func EnableAuditSyslog() (err error) {
	syslogWriter, err = syslog.New(syslog.LOG_ERR, "vtorc")
	if err != nil {
		syslogWriter = nil
	}
	return err
}

func syslogMessage(logMessage string) bool {
	if syslogWriter == nil {
		return false
	}
	go func() {
		_ = syslogWriter.Info(logMessage)
	}()
	return true
}
