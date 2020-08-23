/*
   Copyright 2017 Shlomi Noach, GitHub Inc.

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
	"time"
)

type Downtime struct {
	Key            *InstanceKey
	Owner          string
	Reason         string
	Duration       time.Duration
	BeginsAt       time.Time
	EndsAt         time.Time
	BeginsAtString string
	EndsAtString   string
}

func NewDowntime(instanceKey *InstanceKey, owner string, reason string, duration time.Duration) *Downtime {
	downtime := &Downtime{
		Key:      instanceKey,
		Owner:    owner,
		Reason:   reason,
		Duration: duration,
		BeginsAt: time.Now(),
	}
	downtime.EndsAt = downtime.BeginsAt.Add(downtime.Duration)
	return downtime
}

func (downtime *Downtime) Ended() bool {
	return downtime.EndsAt.Before(time.Now())
}

func (downtime *Downtime) EndsIn() time.Duration {
	return time.Until(downtime.EndsAt)
}
