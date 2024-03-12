/*
Copyright 2023 The Vitess Authors.

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

package evalengine

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type argError string

func (err argError) Error() string {
	return fmt.Sprintf("Incorrect parameter count in the call to native function '%s'", string(err))
}

func (ast *astCompiler) translateFuncArgs(fnargs []sqlparser.Expr) ([]IR, error) {
	var args TupleExpr
	for _, expr := range fnargs {
		convertedExpr, err := ast.translateExpr(expr)
		if err != nil {
			return nil, err
		}
		args = append(args, convertedExpr)
	}
	return args, nil
}

func (ast *astCompiler) translateFuncExpr(fn *sqlparser.FuncExpr) (IR, error) {
	var args TupleExpr
	for _, expr := range fn.Exprs {
		convertedExpr, err := ast.translateExpr(expr)
		if err != nil {
			return nil, err
		}
		args = append(args, convertedExpr)
	}

	method := fn.Name.Lowered()
	call := CallExpr{Arguments: args, Method: method}

	switch method {
	case "isnull":
		return builtinIsNullRewrite(args)
	case "ifnull":
		return builtinIfNullRewrite(args)
	case "nullif":
		return builtinNullIfRewrite(args)
	case "if":
		return builtinIfRewrite(args)
	case "coalesce":
		if len(args) == 0 {
			return nil, argError(method)
		}
		return &builtinCoalesce{CallExpr: call}, nil
	case "greatest":
		if len(args) == 0 {
			return nil, argError(method)
		}
		return &builtinMultiComparison{CallExpr: call, cmp: 1}, nil
	case "least":
		if len(args) == 0 {
			return nil, argError(method)
		}
		return &builtinMultiComparison{CallExpr: call, cmp: -1}, nil
	case "collation":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinCollation{CallExpr: call}, nil
	case "bit_count":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinBitCount{CallExpr: call}, nil
	case "hex":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinHex{CallExpr: call, collate: ast.cfg.Collation}, nil
	case "unhex":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinUnhex{CallExpr: call}, nil
	case "ceil", "ceiling":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinCeil{CallExpr: call}, nil
	case "floor":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinFloor{CallExpr: call}, nil
	case "abs":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinAbs{CallExpr: call}, nil
	case "pi":
		if len(args) != 0 {
			return nil, argError(method)
		}
		return &builtinPi{CallExpr: call}, nil
	case "acos":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinAcos{CallExpr: call}, nil
	case "asin":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinAsin{CallExpr: call}, nil
	case "atan":
		switch len(args) {
		case 1:
			return &builtinAtan{CallExpr: call}, nil
		case 2:
			return &builtinAtan2{CallExpr: call}, nil
		default:
			return nil, argError(method)
		}
	case "atan2":
		if len(args) != 2 {
			return nil, argError(method)
		}
		return &builtinAtan2{CallExpr: call}, nil
	case "cos":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinCos{CallExpr: call}, nil
	case "cot":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinCot{CallExpr: call}, nil
	case "sin":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinSin{CallExpr: call}, nil
	case "tan":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinTan{CallExpr: call}, nil
	case "degrees":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinDegrees{CallExpr: call}, nil
	case "radians":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinRadians{CallExpr: call}, nil
	case "exp":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinExp{CallExpr: call}, nil
	case "ln":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinLn{CallExpr: call}, nil
	case "log":
		switch len(args) {
		case 1:
			return &builtinLn{CallExpr: call}, nil
		case 2:
			return &builtinLog{CallExpr: call}, nil
		default:
			return nil, argError(method)
		}
	case "log10":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinLog10{CallExpr: call}, nil
	case "mod":
		if len(args) != 2 {
			return nil, argError(method)
		}
		return &ArithmeticExpr{
			BinaryExpr: BinaryExpr{
				Left:  args[0],
				Right: args[1],
			},
			Op: &opArithMod{},
		}, nil
	case "log2":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinLog2{CallExpr: call}, nil
	case "pow", "power":
		if len(args) != 2 {
			return nil, argError(method)
		}
		return &builtinPow{CallExpr: call}, nil
	case "sign":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinSign{CallExpr: call}, nil
	case "sqrt":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinSqrt{CallExpr: call}, nil
	case "round":
		switch len(args) {
		case 1, 2:
			return &builtinRound{CallExpr: call}, nil
		default:
			return nil, argError(method)
		}
	case "truncate":
		if len(args) != 2 {
			return nil, argError(method)
		}
		return &builtinTruncate{CallExpr: call}, nil
	case "crc32":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinCrc32{CallExpr: call}, nil
	case "conv":
		if len(args) != 3 {
			return nil, argError(method)
		}
		return &builtinConv{CallExpr: call, collate: ast.cfg.Collation}, nil
	case "bin":
		if len(args) != 1 {
			return nil, argError(method)
		}
		args = append(args, NewLiteralInt(10))
		args = append(args, NewLiteralInt(2))
		var cexpr = CallExpr{Arguments: args, Method: "BIN"}
		return &builtinConv{CallExpr: cexpr, collate: ast.cfg.Collation}, nil
	case "oct":
		if len(args) != 1 {
			return nil, argError(method)
		}
		args = append(args, NewLiteralInt(10))
		args = append(args, NewLiteralInt(8))
		var cexpr = CallExpr{Arguments: args, Method: "OCT"}
		return &builtinConv{CallExpr: cexpr, collate: ast.cfg.Collation}, nil
	case "left", "right":
		if len(args) != 2 {
			return nil, argError(method)
		}
		return &builtinLeftRight{CallExpr: call, collate: ast.cfg.Collation, left: method == "left"}, nil
	case "lpad", "rpad":
		if len(args) != 3 {
			return nil, argError(method)
		}
		return &builtinPad{CallExpr: call, collate: ast.cfg.Collation, left: method == "lpad"}, nil
	case "lower", "lcase":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinChangeCase{CallExpr: call, upcase: false, collate: ast.cfg.Collation}, nil
	case "upper", "ucase":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinChangeCase{CallExpr: call, upcase: true, collate: ast.cfg.Collation}, nil
	case "char_length", "character_length":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinCharLength{CallExpr: call}, nil
	case "length", "octet_length":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinLength{CallExpr: call}, nil
	case "bit_length":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinBitLength{CallExpr: call}, nil
	case "ascii":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinASCII{CallExpr: call}, nil
	case "reverse":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinReverse{CallExpr: call, collate: ast.cfg.Collation}, nil
	case "space":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinSpace{CallExpr: call, collate: ast.cfg.Collation}, nil
	case "ord":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinOrd{CallExpr: call, collate: ast.cfg.Collation}, nil
	case "repeat":
		if len(args) != 2 {
			return nil, argError(method)
		}
		return &builtinRepeat{CallExpr: call, collate: ast.cfg.Collation}, nil
	case "concat":
		if len(args) < 1 {
			return nil, argError(method)
		}
		return &builtinConcat{CallExpr: call, collate: ast.cfg.Collation}, nil
	case "concat_ws":
		if len(args) < 2 {
			return nil, argError(method)
		}
		return &builtinConcatWs{CallExpr: call, collate: ast.cfg.Collation}, nil
	case "from_base64":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinFromBase64{CallExpr: call}, nil
	case "to_base64":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinToBase64{CallExpr: call, collate: ast.cfg.Collation}, nil
	case "json_depth":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinJSONDepth{CallExpr: call}, nil
	case "json_length":
		switch len(args) {
		case 1, 2:
			return &builtinJSONLength{CallExpr: call}, nil
		default:
			return nil, argError(method)
		}
	case "curdate", "current_date":
		if len(args) != 0 {
			return nil, argError(method)
		}
		return &builtinCurdate{CallExpr: call}, nil
	case "utc_date":
		if len(args) != 0 {
			return nil, argError(method)
		}
		return &builtinUtcDate{CallExpr: call}, nil
	case "date_format":
		if len(args) != 2 {
			return nil, argError(method)
		}
		return &builtinDateFormat{CallExpr: call, collate: ast.cfg.Collation}, nil
	case "date":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinDate{CallExpr: call}, nil
	case "dayofmonth", "day":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinDayOfMonth{CallExpr: call}, nil
	case "dayofweek":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinDayOfWeek{CallExpr: call}, nil
	case "dayofyear":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinDayOfYear{CallExpr: call}, nil
	case "from_unixtime":
		switch len(args) {
		case 1, 2:
			return &builtinFromUnixtime{CallExpr: call, collate: ast.cfg.Collation}, nil
		default:
			return nil, argError(method)
		}
	case "hour":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinHour{CallExpr: call}, nil
	case "makedate":
		if len(args) != 2 {
			return nil, argError(method)
		}
		return &builtinMakedate{CallExpr: call}, nil
	case "maketime":
		if len(args) != 3 {
			return nil, argError(method)
		}
		return &builtinMaketime{CallExpr: call}, nil
	case "microsecond":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinMicrosecond{CallExpr: call}, nil
	case "minute":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinMinute{CallExpr: call}, nil
	case "month":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinMonth{CallExpr: call}, nil
	case "monthname":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinMonthName{CallExpr: call, collate: ast.cfg.Collation}, nil
	case "last_day":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinLastDay{CallExpr: call}, nil
	case "to_days":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinToDays{CallExpr: call}, nil
	case "from_days":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinFromDays{CallExpr: call}, nil
	case "time_to_sec":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinTimeToSec{CallExpr: call}, nil
	case "quarter":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinQuarter{CallExpr: call}, nil
	case "second":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinSecond{CallExpr: call}, nil
	case "time":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinTime{CallExpr: call}, nil
	case "unix_timestamp":
		switch len(args) {
		case 0, 1:
			return &builtinUnixTimestamp{CallExpr: call}, nil
		default:
			return nil, argError(method)
		}
	case "week":
		switch len(args) {
		case 1, 2:
			return &builtinWeek{CallExpr: call}, nil
		default:
			return nil, argError(method)
		}
	case "weekday":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinWeekDay{CallExpr: call}, nil
	case "weekofyear":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinWeekOfYear{CallExpr: call}, nil
	case "year":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinYear{CallExpr: call}, nil
	case "yearweek":
		switch len(args) {
		case 1, 2:
			return &builtinYearWeek{CallExpr: call}, nil
		default:
			return nil, argError(method)
		}
	case "inet_aton":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinInetAton{CallExpr: call}, nil
	case "inet_ntoa":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinInetNtoa{CallExpr: call, collate: ast.cfg.Collation}, nil
	case "inet6_aton":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinInet6Aton{CallExpr: call}, nil
	case "inet6_ntoa":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinInet6Ntoa{CallExpr: call, collate: ast.cfg.Collation}, nil
	case "is_ipv4":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinIsIPV4{CallExpr: call}, nil
	case "is_ipv4_compat":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinIsIPV4Compat{CallExpr: call}, nil
	case "is_ipv4_mapped":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinIsIPV4Mapped{CallExpr: call}, nil
	case "is_ipv6":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinIsIPV6{CallExpr: call}, nil
	case "bin_to_uuid":
		switch len(args) {
		case 1, 2:
			return &builtinBinToUUID{CallExpr: call, collate: ast.cfg.Collation}, nil
		default:
			return nil, argError(method)
		}
	case "is_uuid":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinIsUUID{CallExpr: call}, nil
	case "uuid":
		if len(args) != 0 {
			return nil, argError(method)
		}
		return &builtinUUID{CallExpr: call}, nil
	case "uuid_to_bin":
		switch len(args) {
		case 1, 2:
			return &builtinUUIDToBin{CallExpr: call}, nil
		default:
			return nil, argError(method)
		}
	case "user", "current_user", "session_user", "system_user":
		if len(args) != 0 {
			return nil, argError(method)
		}
		return &builtinUser{CallExpr: call}, nil
	case "database", "schema":
		if len(args) != 0 {
			return nil, argError(method)
		}
		return &builtinDatabase{CallExpr: call}, nil
	case "version":
		if len(args) != 0 {
			return nil, argError(method)
		}
		return &builtinVersion{CallExpr: call}, nil
	case "md5":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinMD5{CallExpr: call, collate: ast.cfg.Collation}, nil
	case "random_bytes":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinRandomBytes{CallExpr: call}, nil
	case "sha1", "sha":
		if len(args) != 1 {
			return nil, argError(method)
		}
		return &builtinSHA1{CallExpr: call, collate: ast.cfg.Collation}, nil
	case "sha2":
		if len(args) != 2 {
			return nil, argError(method)
		}
		return &builtinSHA2{CallExpr: call, collate: ast.cfg.Collation}, nil
	case "convert_tz":
		if len(args) != 3 {
			return nil, argError(method)
		}
		return &builtinConvertTz{CallExpr: call}, nil
	case "strcmp":
		if len(args) != 2 {
			return nil, argError(method)
		}
		return &builtinStrcmp{CallExpr: call, collate: ast.cfg.Collation}, nil
	case "instr":
		if len(args) != 2 {
			return nil, argError(method)
		}
		call = CallExpr{Arguments: []IR{call.Arguments[1], call.Arguments[0]}, Method: method}
		return &builtinLocate{CallExpr: call, collate: ast.cfg.Collation}, nil
	case "replace":
		if len(args) != 3 {
			return nil, argError(method)
		}
		return &builtinReplace{CallExpr: call, collate: ast.cfg.Collation}, nil
	default:
		return nil, translateExprNotSupported(fn)
	}
}

func (ast *astCompiler) translateCallable(call sqlparser.Callable) (IR, error) {
	switch call := call.(type) {
	case *sqlparser.FuncExpr:
		return ast.translateFuncExpr(call)

	case *sqlparser.ConvertExpr:
		return ast.translateConvertExpr(call.Expr, call.Type)

	case *sqlparser.ConvertUsingExpr:
		return ast.translateConvertUsingExpr(call)

	case *sqlparser.WeightStringFuncExpr:
		ws := &builtinWeightString{}

		expr, err := ast.translateExpr(call.Expr)
		if err != nil {
			return nil, err
		}
		ws.CallExpr = CallExpr{
			Arguments: []IR{expr},
			Method:    "WEIGHT_STRING",
		}
		if call.As != nil {
			ws.Cast = strings.ToLower(call.As.Type)
			ws.Len = call.As.Length
		}
		return ws, nil

	case *sqlparser.JSONExtractExpr:
		args, err := ast.translateFuncArgs(append([]sqlparser.Expr{call.JSONDoc}, call.PathList...))
		if err != nil {
			return nil, err
		}
		return &builtinJSONExtract{
			CallExpr: CallExpr{
				Arguments: args,
				Method:    "JSON_EXTRACT",
			},
		}, nil

	case *sqlparser.JSONUnquoteExpr:
		arg, err := ast.translateExpr(call.JSONValue)
		if err != nil {
			return nil, err
		}
		return &builtinJSONUnquote{
			CallExpr: CallExpr{
				Arguments: []IR{arg},
				Method:    "JSON_UNQUOTE",
			},
		}, nil

	case *sqlparser.JSONObjectExpr:
		var args []IR
		for _, param := range call.Params {
			key, err := ast.translateExpr(param.Key)
			if err != nil {
				return nil, err
			}
			val, err := ast.translateExpr(param.Value)
			if err != nil {
				return nil, err
			}
			args = append(args, key, val)
		}
		return &builtinJSONObject{
			CallExpr: CallExpr{
				Arguments: args,
				Method:    "JSON_OBJECT",
			},
		}, nil

	case *sqlparser.JSONArrayExpr:
		args, err := ast.translateFuncArgs(call.Params)
		if err != nil {
			return nil, err
		}
		return &builtinJSONArray{CallExpr: CallExpr{
			Arguments: args,
			Method:    "JSON_ARRAY",
		}}, nil

	case *sqlparser.JSONContainsPathExpr:
		exprs := []sqlparser.Expr{call.JSONDoc, call.OneOrAll}
		exprs = append(exprs, call.PathList...)
		args, err := ast.translateFuncArgs(exprs)
		if err != nil {
			return nil, err
		}
		return &builtinJSONContainsPath{CallExpr: CallExpr{
			Arguments: args,
			Method:    "JSON_CONTAINS_PATH",
		}}, nil

	case *sqlparser.JSONKeysExpr:
		var args []IR
		doc, err := ast.translateExpr(call.JSONDoc)
		if err != nil {
			return nil, err
		}
		args = append(args, doc)

		if call.Path != nil {
			path, err := ast.translateExpr(call.Path)
			if err != nil {
				return nil, err
			}
			args = append(args, path)
		}

		return &builtinJSONKeys{CallExpr: CallExpr{
			Arguments: args,
			Method:    "JSON_KEYS",
		}}, nil

	case *sqlparser.CurTimeFuncExpr:
		if call.Fsp > 6 {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Too-big precision %d specified for '%s'. Maximum is 6.", call.Fsp, call.Name.String())
		}

		var cexpr = CallExpr{Arguments: nil, Method: call.Name.String()}
		var utc, onlyTime bool
		switch call.Name.Lowered() {
		case "current_time", "curtime":
			onlyTime = true
		case "utc_time":
			onlyTime = true
			utc = true
		case "utc_timestamp":
			utc = true
		case "sysdate":
			return &builtinSysdate{
				CallExpr: cexpr,
				prec:     uint8(call.Fsp),
			}, nil
		}
		return &builtinNow{
			CallExpr: cexpr,
			utc:      utc,
			onlyTime: onlyTime,
			prec:     uint8(call.Fsp),
		}, nil

	case *sqlparser.TrimFuncExpr:
		var args []IR
		str, err := ast.translateExpr(call.StringArg)
		if err != nil {
			return nil, err
		}
		args = append(args, str)
		if call.TrimArg != nil {
			trim, err := ast.translateExpr(call.TrimArg)
			if err != nil {
				return nil, err
			}
			args = append(args, trim)
		}

		var cexpr = CallExpr{Arguments: args, Method: call.TrimFuncType.ToString()}
		return &builtinTrim{
			CallExpr: cexpr,
			collate:  ast.cfg.Collation,
			trim:     call.Type,
		}, nil

	case *sqlparser.SubstrExpr:
		var args []IR
		str, err := ast.translateExpr(call.Name)
		if err != nil {
			return nil, err
		}
		args = append(args, str)
		pos, err := ast.translateExpr(call.From)
		if err != nil {
			return nil, err
		}
		args = append(args, pos)

		if call.To != nil {
			to, err := ast.translateExpr(call.To)
			if err != nil {
				return nil, err
			}
			args = append(args, to)
		}
		var cexpr = CallExpr{Arguments: args, Method: "SUBSTRING"}
		return &builtinSubstring{
			CallExpr: cexpr,
			collate:  ast.cfg.Collation,
		}, nil
	case *sqlparser.LocateExpr:
		var args []IR
		substr, err := ast.translateExpr(call.SubStr)
		if err != nil {
			return nil, err
		}
		args = append(args, substr)
		str, err := ast.translateExpr(call.Str)
		if err != nil {
			return nil, err
		}
		args = append(args, str)

		if call.Pos != nil {
			to, err := ast.translateExpr(call.Pos)
			if err != nil {
				return nil, err
			}
			args = append(args, to)
		}
		var cexpr = CallExpr{Arguments: args, Method: "LOCATE"}
		return &builtinLocate{
			CallExpr: cexpr,
			collate:  ast.cfg.Collation,
		}, nil
	case *sqlparser.IntervalDateExpr:
		var err error
		args := make([]IR, 2)

		args[0], err = ast.translateExpr(call.Date)
		if err != nil {
			return nil, err
		}
		args[1], err = ast.translateExpr(call.Interval)
		if err != nil {
			return nil, err
		}

		cexpr := CallExpr{Arguments: args, Method: call.FnName()}
		return &builtinDateMath{
			CallExpr: cexpr,
			sub:      call.IsSubtraction(),
			unit:     call.NormalizedUnit(),
			collate:  ast.cfg.Collation,
		}, nil

	case *sqlparser.RegexpLikeExpr:
		input, err := ast.translateExpr(call.Expr)
		if err != nil {
			return nil, err
		}

		pattern, err := ast.translateExpr(call.Pattern)
		if err != nil {
			return nil, err
		}

		args := []IR{input, pattern}

		if call.MatchType != nil {
			matchType, err := ast.translateExpr(call.MatchType)
			if err != nil {
				return nil, err
			}
			args = append(args, matchType)
		}

		return &builtinRegexpLike{
			CallExpr: CallExpr{Arguments: args, Method: "REGEXP_LIKE"},
			Negate:   false,
		}, nil

	case *sqlparser.RegexpInstrExpr:
		input, err := ast.translateExpr(call.Expr)
		if err != nil {
			return nil, err
		}

		pattern, err := ast.translateExpr(call.Pattern)
		if err != nil {
			return nil, err
		}

		args := []IR{input, pattern}

		if call.Position != nil {
			position, err := ast.translateExpr(call.Position)
			if err != nil {
				return nil, err
			}
			args = append(args, position)
		}

		if call.Occurrence != nil {
			occurrence, err := ast.translateExpr(call.Occurrence)
			if err != nil {
				return nil, err
			}
			args = append(args, occurrence)
		}

		if call.ReturnOption != nil {
			returnOption, err := ast.translateExpr(call.ReturnOption)
			if err != nil {
				return nil, err
			}
			args = append(args, returnOption)
		}

		if call.MatchType != nil {
			matchType, err := ast.translateExpr(call.MatchType)
			if err != nil {
				return nil, err
			}
			args = append(args, matchType)
		}

		return &builtinRegexpInstr{
			CallExpr: CallExpr{Arguments: args, Method: "REGEXP_INSTR"},
		}, nil

	case *sqlparser.RegexpSubstrExpr:
		input, err := ast.translateExpr(call.Expr)
		if err != nil {
			return nil, err
		}

		pattern, err := ast.translateExpr(call.Pattern)
		if err != nil {
			return nil, err
		}

		args := []IR{input, pattern}

		if call.Position != nil {
			position, err := ast.translateExpr(call.Position)
			if err != nil {
				return nil, err
			}
			args = append(args, position)
		}

		if call.Occurrence != nil {
			occurrence, err := ast.translateExpr(call.Occurrence)
			if err != nil {
				return nil, err
			}
			args = append(args, occurrence)
		}

		if call.MatchType != nil {
			matchType, err := ast.translateExpr(call.MatchType)
			if err != nil {
				return nil, err
			}
			args = append(args, matchType)
		}

		return &builtinRegexpSubstr{
			CallExpr: CallExpr{Arguments: args, Method: "REGEXP_SUBSTR"},
		}, nil

	case *sqlparser.RegexpReplaceExpr:
		input, err := ast.translateExpr(call.Expr)
		if err != nil {
			return nil, err
		}

		pattern, err := ast.translateExpr(call.Pattern)
		if err != nil {
			return nil, err
		}

		repl, err := ast.translateExpr(call.Repl)
		if err != nil {
			return nil, err
		}

		args := []IR{input, pattern, repl}

		if call.Position != nil {
			position, err := ast.translateExpr(call.Position)
			if err != nil {
				return nil, err
			}
			args = append(args, position)
		}

		if call.Occurrence != nil {
			occurrence, err := ast.translateExpr(call.Occurrence)
			if err != nil {
				return nil, err
			}
			args = append(args, occurrence)
		}

		if call.MatchType != nil {
			matchType, err := ast.translateExpr(call.MatchType)
			if err != nil {
				return nil, err
			}
			args = append(args, matchType)
		}

		return &builtinRegexpReplace{
			CallExpr: CallExpr{Arguments: args, Method: "REGEXP_REPLACE"},
		}, nil

	case *sqlparser.InsertExpr:
		str, err := ast.translateExpr(call.Str)
		if err != nil {
			return nil, err
		}

		pos, err := ast.translateExpr(call.Pos)
		if err != nil {
			return nil, err
		}

		len, err := ast.translateExpr(call.Len)
		if err != nil {
			return nil, err
		}

		newstr, err := ast.translateExpr(call.NewStr)
		if err != nil {
			return nil, err
		}

		args := []IR{str, pos, len, newstr}

		var cexpr = CallExpr{Arguments: args, Method: "INSERT"}
		return &builtinInsert{
			CallExpr: cexpr,
			collate:  ast.cfg.Collation,
		}, nil
	case *sqlparser.CharExpr:
		args := make([]IR, 0, len(call.Exprs))
		for _, expr := range call.Exprs {
			arg, err := ast.translateExpr(expr)
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
		}

		var cexpr = CallExpr{Arguments: args, Method: "CHAR"}
		var coll collations.ID
		if call.Charset == "" {
			coll = collations.CollationBinaryID
		} else {
			var err error
			coll, err = ast.translateConvertCharset(call.Charset, false)
			if err != nil {
				return nil, err
			}
		}

		return &builtinChar{
			CallExpr: cexpr,
			collate:  coll,
		}, nil
	default:
		return nil, translateExprNotSupported(call)
	}
}

func builtinJSONExtractUnquoteRewrite(left IR, right IR) (IR, error) {
	extract, err := builtinJSONExtractRewrite(left, right)
	if err != nil {
		return nil, err
	}
	return &builtinJSONUnquote{
		CallExpr: CallExpr{
			Arguments: []IR{extract},
			Method:    "JSON_UNQUOTE",
		},
	}, nil
}

func builtinJSONExtractRewrite(left IR, right IR) (IR, error) {
	if _, ok := left.(*Column); !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "lhs of a JSON extract operator must be a column")
	}
	return &builtinJSONExtract{
		CallExpr: CallExpr{
			Arguments: []IR{left, right},
			Method:    "JSON_EXTRACT",
		},
	}, nil
}

func builtinIsNullRewrite(args []IR) (IR, error) {
	if len(args) != 1 {
		return nil, argError("ISNULL")
	}
	return &IsExpr{
		UnaryExpr: UnaryExpr{args[0]},
		Op:        sqlparser.IsNullOp,
		Check:     func(e eval) bool { return e == nil },
	}, nil
}

func builtinIfNullRewrite(args []IR) (IR, error) {
	if len(args) != 2 {
		return nil, argError("IFNULL")
	}
	return &CaseExpr{
		cases: []WhenThen{{
			when: &IsExpr{
				UnaryExpr: UnaryExpr{args[0]},
				Op:        sqlparser.IsNullOp,
				Check: func(e eval) bool {
					return e == nil
				},
			},
			then: args[1],
		}},
		Else: args[0],
	}, nil
}

func builtinNullIfRewrite(args []IR) (IR, error) {
	if len(args) != 2 {
		return nil, argError("NULLIF")
	}
	return &CaseExpr{
		cases: []WhenThen{{
			when: &ComparisonExpr{
				BinaryExpr: BinaryExpr{
					Left:  args[0],
					Right: args[1],
				},
				Op: compareEQ{},
			},
			then: NullExpr,
		}},
		Else: args[0],
	}, nil
}

func builtinIfRewrite(args []IR) (IR, error) {
	if len(args) != 3 {
		return nil, argError("IF")
	}
	return &CaseExpr{
		cases: []WhenThen{{
			when: &IsExpr{
				UnaryExpr: UnaryExpr{args[0]},
				Op:        sqlparser.IsTrueOp,
				Check: func(e eval) bool {
					return evalIsTruthy(e) == boolTrue
				},
			},
			then: args[1],
		}},
		Else: args[2],
	}, nil
}
