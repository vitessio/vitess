package evalengine

import (
	"math"
	"testing"
)

func Test_buildinAbs_call(t *testing.T) {
	type args struct {
		env    *ExpressionEnv
		args   []EvalResult
		result *EvalResult
	}

	int1 := newEvalInt64(0)
	int2 := newEvalInt64(1)
	int3 := newEvalInt64(-2)
	int4 := newEvalInt64(-9223372036854775807)
	//临界值处理
	int5 := newEvalInt64(math.MinInt64)

	dec1 := newEvalFloat(-1.2)
	dec2 := newEvalFloat(12.5)

	tests := []struct {
		name string
		args args
		err  string
	}{
		{name: "test_int64_(0)", args: args{
			env: nil,
			args: []EvalResult{
				int1,
			},
			result: &EvalResult{},
		}},
		{name: "test_int64_(1)", args: args{
			env: nil,
			args: []EvalResult{
				int2,
			},
			result: &EvalResult{},
		}},
		{name: "test_int64_(-2)", args: args{
			env: nil,
			args: []EvalResult{
				int3,
			},
			result: &EvalResult{},
		}}, {name: "test_int64_(-9223372036854775807)", args: args{
			env: nil,
			args: []EvalResult{
				int4,
			},
			result: &EvalResult{},
		},
		},
		{name: "test_int64_(math.MinInt64)", args: args{
			env: nil,
			args: []EvalResult{
				int5,
			},
			result: &EvalResult{},
		},
		},
		{name: "test_float_(-1.2)", args: args{
			env: nil,
			args: []EvalResult{
				dec1,
			},
			result: &EvalResult{},
		},
		}, {name: "test_float_(12.5)", args: args{
			env: nil,
			args: []EvalResult{
				dec2,
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
			bu := builtinAbs{}
			bu.call(tt.args.env, tt.args.args, tt.args.result)
		})
	}
}
