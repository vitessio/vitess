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

package util

import (
	"fmt"
	"time"

	"github.com/patrickmn/go-cache"
)

var logEntryCache *cache.Cache = cache.New(time.Minute, time.Second*5)

func ClearToLog(topic string, key string) bool {
	return logEntryCache.Add(fmt.Sprintf("%s:%s", topic, key), true, cache.DefaultExpiration) == nil
}
