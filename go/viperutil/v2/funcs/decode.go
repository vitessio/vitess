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
