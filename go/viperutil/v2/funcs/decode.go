/*
Copyright 2022 The Vitess Authors.

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

package funcs

// TODO: this creates an import cycle ... IMO tabletenv should not define Seconds,
// it should be in something more akin to flagutil with the other values like
// TabletTypeFlag and friends.

// import (
// 	"reflect"
// 	"time"
//
// 	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
// )
//
// func DecodeSeconds(from, to reflect.Type, data any) (any, error) {
// 	if to != reflect.TypeOf(tabletenv.Seconds(0)) {
// 		return data, nil
// 	}
//
// 	var (
// 		n  float64
// 		ok bool
// 	)
// 	switch from.Kind() {
// 	case reflect.Float32:
// 		data := data.(float32)
// 		n, ok = float64(data), true
// 	case reflect.Float64:
// 		n, ok = data.(float64), true
// 	}
//
// 	if ok {
// 		return tabletenv.Seconds(n), nil
// 	}
//
// 	if from != reflect.TypeOf(time.Duration(0)) {
// 		return data, nil
// 	}
//
// 	s := tabletenv.Seconds(0)
// 	s.Set(data.(time.Duration))
// 	return s, nil
// }

/* USAGE (similar to how ConfigFileNotFoundHandling is decoded in viperutil/config.go)
// viper.DecoderConfigOption(viper.DecodeHook(mapstructure.DecodeHookFunc(decode.Seconds))),
// viper.DecoderConfigOption(func(dc *mapstructure.DecoderConfig) {
// 	dc
// }),
*/
