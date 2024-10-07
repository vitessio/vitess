package rd

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"regexp"
	"strconv"
	"strings"
)

func rewriteTypes(infile io.Reader, outFile io.Writer) (err error) {
	sc := bufio.NewScanner(infile)

	var goTypes []goType
	var yaccTypes []yaccType
	//var defs = make(map[string]*def)
	//var defNames []string

	var goTypesMap = make(map[string]string)
	var funcExprsMap = make(map[string]string)
	//computeTypeLookups := func() error {
	//	for _, typ := range goTypes {
	//		goTypesMap[typ.name] = typ.typ
	//	}
	//	for _, d := range yaccTypes {
	//		goTyp, ok := goTypesMap[d.typ]
	//		if !ok {
	//			return fmt.Errorf("function expression not found: %s", d.name)
	//		}
	//		funcExprsMap[d.name] = goTyp
	//	}
	//	return nil
	//}

	//varToType := func(fields []string) []string {
	//	typs := make([]string, len(fields)+1)
	//	var ok bool
	//	for i, f := range fields {
	//		if f == "';'" {
	//			typs[i+1] = "[]byte"
	//		} else {
	//			typs[i+1], ok = funcExprsMap[f]
	//			if !ok {
	//				log.Fatalf("missing rule/terminal: %s", f)
	//			}
	//		}
	//	}
	//	return typs
	//}

	castVarTypes := func(l string, typs []string) string {
		match := variableRe.FindAllStringSubmatchIndex(l, 1)
		for len(match) > 0 {
			m := match[0]
			start, end := m[4], m[5]

			_int64, err := strconv.ParseInt(l[start:end], 10, 64)
			if err != nil {
				log.Fatalf("failed to parse variable string: %s", l[start:end])
			}
			if _int64 >= 64 {
				log.Fatalf("variable reference too big: %d", _int64)
			}
			//l = l[:start-1] + "var" + l[start:]
			typ := typs[_int64-1]
			if typ == "[]byte" {
				l = fmt.Sprintf("%svarx%s%s", l[:start-1], l[start:end], l[end:])

			} else {
				l = fmt.Sprintf("%svarx%s.(%s)%s", l[:start-1], l[start:end], typ, l[end:])
			}
			match = variableRe.FindAllStringSubmatchIndex(l, 1)
		}
		l = strings.ReplaceAll(l, "varx", "$")
		return l
	}

	//castReturn := func(l string, typ string) string {
	//	return strings.ReplaceAll(l, "$$", "$$.("+typ+")")
	//}

	//printDef := func(d *def) {
	//	defNames = append(defNames, d.name)
	//	defs[d.name] = d
	//	if _, ok := funcExprsMap[d.name]; !ok {
	//		funcExprsMap[d.name] = "[]byte"
	//	}
	//
	//	fmt.Println(outFile, d.name+":")
	//	for _, r := range d.rules.flat {
	//		r.fields = strings.Fields(r.name)
	//		typs := varToType(r.fields)
	//		for _, l := range r.body {
	//			l = castVarTypes(l, typs)
	//			fmt.Println(outFile, l)
	//		}
	//	}
	//}

	//addDef := func(d *def) {
	//	defs[d.name] = d
	//	defNames = append(defNames, d.name)
	//}

	var ruleStart bool = false

	for !ruleStart && sc.Scan() {
		line := sc.Text()

		switch line {
		case "%union {":
			fmt.Fprintln(outFile, line)
			fmt.Fprintln(outFile, "    val interface{}")
			fmt.Fprintln(outFile, "    bytes []byte")
			for sc.Scan() {
				line := sc.Text()
				if line == "}" {
					fmt.Fprintln(outFile, line)
					break
				}
				parts := strings.Fields(line)
				if len(parts) != 2 {
					return
				}
				gt := goType{name: parts[0], typ: parts[1]}
				goTypesMap[gt.name] = gt.typ
				goTypes = append(goTypes, gt)
				//fmt.Fprintln(outFile, line)
			}
			continue
		case "%{":
			fmt.Fprintln(outFile, line)
			for sc.Scan() {
				line := sc.Text()
				fmt.Fprintln(outFile, line)
				if line == "%}" {
					break
				}
			}
			continue
		case "%%":
			ruleStart = true
		default:
		}

		if strings.HasPrefix(line, "%type") {
			yaccTyp, err := parseYaccType(line)
			if err != nil {
				return err
			}
			for _, t := range yaccTyp {
				funcExprsMap[t.name] = goTypesMap[t.typ]
			}
			yaccTypes = append(yaccTypes, yaccTyp...)
			line = replaceTypeWithVal(line)
		}
		fmt.Fprintln(outFile, line)
	}

	// rules

	var r *rule
	var d *def

	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			fmt.Fprintln(outFile, line)
			continue
		}
		if strings.HasSuffix(line, ":") && !strings.HasPrefix(strings.TrimSpace(line), "case ") {
			r = nil
			d = new(def)
			d.name = line[:len(line)-1]
			fmt.Println(d.name)
		} else if r == nil {
			if line == "{}" || line == "" || line == "{" || line == "/*empty*/" {
				print()
			} else {
				r = new(rule)
				r.name = parseRuleName(line)
				r.fields = strings.Fields(r.name)
				r.types = parseRuleTypes(r.fields, funcExprsMap)
			}
		} else if strings.HasPrefix(line, "|") {
			r = new(rule)
			r.name = parseRuleName(line)
			r.fields = strings.Fields(r.name)
			r.types = parseRuleTypes(r.fields, funcExprsMap)
		} else {
			// var replace
			line = castVarTypes(line, r.types)
			t, ok := funcExprsMap[d.name]
			if !ok {
				t = "[]byte"
			}
			line = strings.ReplaceAll(line, "$$.", "$$.("+t+").")
			line = strings.ReplaceAll(line, "append($$,", "append($$.("+t+"),")
			if strings.HasPrefix(t, "[]") || listTypes[t] {
				line = strings.ReplaceAll(line, "$$ = nil", "$$ = "+t+"(nil)")
			} else if strings.HasPrefix(t, "*") {
				line = strings.ReplaceAll(line, "$$ = nil", "$$ = &"+t[1:]+"{}")
			} else {
				line = strings.ReplaceAll(line, "$$ = nil", "$$ = "+t+"(nil)")
			}
		}

		fmt.Fprintln(outFile, line)
	}

	return nil
}

var listTypes = map[string]bool{
	"Partitions":      true,
	"Columns":         true,
	"AssignmentExprs": true,
	"Exprs":           true,
	"SelectExprs":     true,
	"Statements":      true,
	"Variables":       true,
	"TableExprs":      true,
	"TableNames":      true,
	"AliasedValues":   true,
	"SetVarExprs":     true,
	"PurgeBinaryLogs": true,
}

var typeRe = regexp.MustCompile("<[a-zA-Z]+[0-9]*>")

func replaceTypeWithVal(s string) string {
	m := typeRe.FindStringIndex(s)
	start := m[0]
	end := m[1]
	if s[start:end] == "<bytes>" {
		return s
	}
	return s[:start] + "<val>" + s[end:]
}

func parseRuleTypes(fields []string, funcExprsMap map[string]string) []string {
	var typs []string
	for _, f := range fields {
		t, ok := funcExprsMap[f]
		if !ok {
			t = "[]byte"
		}
		typs = append(typs, t)
	}
	return typs
}
