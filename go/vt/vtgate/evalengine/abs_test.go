package evalengine

import (
	"math"
	"testing"
)

func Test_buildinABS_call(t *testing.T) {
	type args struct {
		env    *ExpressionEnv
		args   []EvalResult
		result *EvalResult
	}

	int1 := newEvalInt64(-2)
	int2 := newEvalInt64(-9223372036854775807)
	//临界值处理
	int3 := newEvalInt64(math.MinInt64)
	dec1, _ := newDecimalString("-0.12")
	dec2, _ := newDecimalString("12.5")

	tests := []struct {
		name string
		args args
		err  string
	}{
		{name: "test_int64_(-2)", args: args{
			env: nil,
			args: []EvalResult{
				int1,
			},
			result: &EvalResult{},
		}}, {name: "test_int64_(-9223372036854775807)", args: args{
			env: nil,
			args: []EvalResult{
				int2,
			},
			result: &EvalResult{},
		},
		},
		{name: "test_int64_(math.MinInt64)", args: args{
			env: nil,
			args: []EvalResult{
				int3,
			},
			result: &EvalResult{},
		},
		},
		{name: "test_decimal_(-0.12)", args: args{
			env: nil,
			args: []EvalResult{
				newEvalDecimal(dec1),
			},
			result: &EvalResult{},
		},
		}, {name: "test_decimal_(12.5)", args: args{
			env: nil,
			args: []EvalResult{
				newEvalDecimal(dec2),
			},
			result: &EvalResult{},
		},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if err := recover(); err != nil {

				}
			}()
			bu := buildinABS{}
			bu.call(tt.args.env, tt.args.args, tt.args.result)
		})
	}
}
