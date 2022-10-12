/*
Copyright 2022 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package viperget

import (
	"time"

	"github.com/spf13/viper"
)

func DurationOrInt(v *viper.Viper, intFallbackUnit time.Duration) func(key string) time.Duration {
	if v == nil {
		v = viper.GetViper()
	}

	return func(key string) time.Duration {
		if !v.IsSet(key) {
			return time.Duration(0)
		}

		if d := v.GetDuration(key); d != 0 {
			return d
		}

		return time.Duration(v.GetInt(key)) * intFallbackUnit
	}
}
