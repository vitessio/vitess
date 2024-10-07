package rd

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// split yacc file into individual functions
// remove comments
// split

type yaccFileContents struct {
	prefix    []string
	goTypes   []goType
	yaccTypes []yaccType
	tokens    []yaccToken
	start     string
	defs      map[string]*def
	defNames  []string
}

func (yc *yaccFileContents) addDef(d *def) {
	yc.defNames = append(yc.defNames, d.name)
	yc.defs[d.name] = d
	return
}

type goType struct {
	name string
	typ  string
}

type yaccType struct {
	name string
	typ  string
}

type yaccToken struct {
	name    string
	yaccTyp string
}

type def struct {
	name  string
	rules *rulePrefix
	done  bool
}

func newDef() *def {
	d := new(def)
	d.rules = new(rulePrefix)
	return d
}

type rule struct {
	name     string
	fields   []string
	types    []string
	set      bool
	body     []string
	usedVars int64
}

type rulePrefix struct {
	prefix   string
	flat     []*rule
	term     []*rule
	pref     []*rulePrefix
	rec      *rulePrefix
	empty    *rule
	usedVars int64
	done     bool
}

func split(infile io.Reader) (yc *yaccFileContents, err error) {
	sc := bufio.NewScanner(infile)
	var buf []string
	var acc bool
	var d *def
	var r *rule
	var nesting int
	yc = new(yaccFileContents)
	yc.defs = make(map[string]*def)
	defer func() {
		if err != nil {
			return
		}
		err = yc.finalize()
	}()
	for sc.Scan() {
		line := sc.Text()
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		if line == "/*empty*/" {
			print()
		}

		line = strings.ReplaceAll(line, "%prec", "")

		switch line {
		case "{":
			if r == nil {
				r = new(rule)
			}
			r.set = true

			continue
		case "%{":
			acc = true
			continue
		case "%}":
			yc.prefix = buf
			buf = nil
			acc = false
			continue
		case "%union {":
			acc = true
			continue
		case "},", "})":
			nesting--
		case "}":
			if nesting > 0 {
				nesting--
			} else if d != nil {
				d.rules.addRule(r)
				r = nil
				continue
			} else {
				yc.goTypes, err = parseGoTypes(buf)
				if err != nil {
					return nil, err
				}
				buf = nil
				acc = false
				continue
			}
		case "%%":
			continue
		case "{}":
			if d != nil {
				r := new(rule)
				r.set = true
				r.body = []string{"$$ = nil"}
				d.rules.empty = r
			}
			continue
		}

		if line[0] == '{' && line[len(line)-1] == '}' {
			if r == nil {
				r = new(rule)
			}
			r.set = true
			r.body = append(r.body, strings.TrimSpace(line[1:len(line)-1]))
			d.rules.addRule(r)
			r = nil
			continue
		}

		if acc {
			if line[len(line)-1] == '{' {
				nesting++
			}
			buf = append(buf, line)
		}

		if strings.HasPrefix(line, "%left") {
		} else if strings.HasPrefix(line, "%type") {
			yaccTyp, err := parseYaccType(line)
			if err != nil {
				return nil, err
			}
			yc.yaccTypes = append(yc.yaccTypes, yaccTyp...)
		} else if strings.HasPrefix(line, "%token") {
			//yc.tokens = append(yc.tokens, line)
			continue
		} else if strings.HasPrefix(line, "%right") {

		} else if strings.HasPrefix(line, "%nonassoc") {
		} else if strings.HasPrefix(line, "//") {
			continue
		} else if strings.HasPrefix(line, "%start") {
			yc.start = strings.Split(line, " ")[1]
		} else if strings.HasPrefix(line, "|") && r != nil {
			d.rules.addRule(r)
			r = nil
		} else if strings.HasPrefix(line, "} else") && r != nil {
			nesting--
		}

		if line[len(line)-1] == ':' && !strings.HasPrefix(line, "case ") {
			if r != nil {
				d.rules.addRule(r)
				r = nil
			}
			if d != nil {
				yc.addDef(d)
			}
			d = newDef()
			d.name = line[:len(line)-1]
			continue
		}
		if r != nil {
			if line[len(line)-1] == '{' {
				nesting++
			}
			r.body = append(r.body, line)
		} else if d != nil {
			r = new(rule)
			r.set = true
			if strings.HasSuffix(line, "{}") {
				line = line[:len(line)-2]
				r.name = parseRuleName(line)
				d.rules.addRule(r)
				r = nil
				continue
			}
			r.name = parseRuleName(line)
		}
	}
	if r != nil {
		d.rules.addRule(r)
	}
	if d != nil {
		yc.addDef(d)
	}
	return yc, nil
}

func (yc *yaccFileContents) finalize() error {
	for _, d := range yc.defs {
		if err := d.rules.finalize(d.name, yc.defs); err != nil {
			return err
		}
	}
	return nil
}

func (r *rule) calcUsed() error {
	if len(r.body) == 0 {
		r.usedVars |= 1 << 1
	}
	for i, b := range r.body {
		newB, used, err := normalizeBodyLine(b)
		if err != nil {
			return err
		}
		r.usedVars |= used
		r.body[i] = newB
	}
	return nil
}

func (d *rulePrefix) calcUsed() {
	for _, t := range d.flat {
		d.usedVars |= t.usedVars
	}
}

func (d *rulePrefix) addRule(r *rule) {
	d.flat = append(d.flat, r)
}

func (d *rulePrefix) recurseChildren(name string, defs map[string]*def) error {
	// expand/partition children first
	for _, r := range d.flat {
		for _, f := range r.fields {
			if d, ok := defs[f]; ok {
				if err := d.rules.finalize(f, defs); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (d *rulePrefix) expandConflicts(name string, defs map[string]*def) {
	// fixed point iteration, weasel out conflicts for this prefix level
	// look at first field, store which we've seen

	// token in r, conflicting starting token in r2
	// two different r2's have conflicting starting token
	nextTokens := make(map[string]string)
	for _, r := range d.flat {
		// toplevel next tokens
		if len(r.fields) == 0 {
			continue
		}
		nextTokens[r.fields[0]] = r.name
	}
	ignore := make(map[string]bool)

	// don't expand self-recursive
	ignore[name] = true
	if name == "table_name_list" {
		print()
	}
	for {
		var conflicts []*rule
		for _, r := range d.flat {
			// find nested conflicts
			if d, ok := defs[r.name]; ok {
				// next token is a function, go one deeper
				for _, r2 := range d.rules.flat {
					// todo flat needs all fields
					if len(r2.fields) == 0 || r2.name == r.name {
						continue
					}
					if rc, ok := nextTokens[r2.fields[0]]; ok && rc != r.name && !ignore[r2.fields[0]] {
						// lookahead conflict
						// advance rule
						if name == "table_name_list" {
							print()
						}
						_, ok := defs[r2.fields[0]]
						nr2 := r2.copy()
						if len(r.body) > 0 && ok && len(r2.body) > 0 {
							// previous return is wrong type
							// rename |ret| to |varx|
							// replace |var1| with |varx| in |r.body|
							for i, l := range nr2.body {
								if l == "ret = BoolVal(false)" {
									print()
								}
								l = strings.ReplaceAll(l, "ret = ", "varx := ")
								l = strings.ReplaceAll(l, "ret.", "varx.")
								nr2.body[i] = l
							}
							callerBody := make([]string, len(r.body))
							for i, l := range r.body {
								if strings.Contains(l, "yylex.Error") {
									callerBody = []string{l, "return ret, false"}
									nr2.body = nil
									break
								}
								l = strings.ReplaceAll(l, "var1", "varx")
								callerBody[i] = l
							}
							nr2.body = append(nr2.body, callerBody...)
						} else {
							// same type return
						}
						nr2.calcUsed()
						conflicts = append(conflicts, nr2)
					}
				}
				for _, r2 := range d.rules.flat {
					// catch inter-child-rule conflicts
					// this takes two cycles to unwind
					if len(r2.fields) == 0 {
						continue
					}
					nextTokens[r2.fields[0]] = r2.name
				}
				nextTokens[d.name] = d.name
			}
		}
		if len(conflicts) == 0 {
			break
		}
		// remove
		for _, r := range conflicts {
			// prevent looping
			ignore[r.fields[0]] = true
		}
		d.flat = append(d.flat, conflicts...)
		conflicts = conflicts[:0]
	}
	return
}

func (d *rulePrefix) finalize(name string, defs map[string]*def) error {
	if d.done {
		return nil
	}
	d.done = true

	for _, r := range d.flat {
		if err := r.calcUsed(); err != nil {
			return err
		}
		r.fields = strings.Fields(r.name)
	}

	if err := d.recurseChildren(name, defs); err != nil {
		return err
	}

	d.expandConflicts(name, defs)

	if name == "any_command" {
		print()
	}

	if err := d.replaceLeftRecursion(name); err != nil {
		return err
	}

	return d.partition(name)
}

func (d *rulePrefix) replaceLeftRecursion(name string) error {
	if name == "tuple_list" {
		print()
	}
	// call this before partition
	term := make(map[string]bool)
	var recStart int = -1
	var recEnd = -1
	for i := 0; i < len(d.flat); i++ {
		r := d.flat[i]
		if fs := r.fields; len(fs) > 0 && fs[0] == name {
			if recStart < 0 {
				recStart = i
			}
			continue
		}
		if recStart > 0 && recEnd < 0 {
			recEnd = i
			continue
		}
		term[strings.Join(r.fields, " ")] = true
	}
	if recStart < 0 {
		return nil
	}
	if recEnd < 0 {
		recEnd = len(d.flat)
	}

	//newRec := make([]*rule, recEnd-recStart)
	for i := recStart; i < recEnd; i++ {
		r := d.flat[i]
		var match bool
		suffix := strings.Join(r.fields[1:], " ")
		if term[suffix] {
			// direct left recursion
			// expr -> (term) | (expr term)
			//nr := r.copy()
			match = true
			r.fields = append(r.fields[1:], name)
			totalFields := len(strings.Fields(r.name))
			offset := totalFields - len(r.fields)
			from := fmt.Sprintf("var%d", offset+1)
			to := fmt.Sprintf("var%d", totalFields)
			for i, l := range r.body {
				// XXX: this only works when recursive list rules are <10 fields
				l = strings.ReplaceAll(l, to, "varx")
				l = strings.ReplaceAll(l, from, to)
				l = strings.ReplaceAll(l, "varx", from)
				l = strings.ReplaceAll(l, "append(ret", "append("+to)
				r.body[i] = l
			}
			r.calcUsed()
		} else if term[r.fields[len(r.fields)-1]] {
			match = true
			r.fields = append(r.fields[:len(r.fields)-1], name)
			totalFields := len(strings.Fields(r.name))
			offset := totalFields - len(r.fields)
			from := fmt.Sprintf("var%d", offset+1)
			to := fmt.Sprintf("var%d", totalFields)
			for i, l := range r.body {
				// XXX: this only works when recursive list rules are <10 fields
				l = strings.ReplaceAll(l, to, "varx")
				l = strings.ReplaceAll(l, from, to)
				l = strings.ReplaceAll(l, "varx", from)
				l = strings.ReplaceAll(l, "append(ret", "append("+to)
				l = strings.ReplaceAll(l, fmt.Sprintf("append(%s, %s)", to, from), fmt.Sprintf("append(%s, %s...)", to, from))
				r.body[i] = l
			}
			r.calcUsed()
		}
		if match {
			totalFields := len(strings.Fields(r.name))
			offset := totalFields - len(r.fields)
			from := fmt.Sprintf("var%d", offset+1)
			to := fmt.Sprintf("var%d", totalFields)
			for i, l := range r.body {
				// XXX: this only works when recursive list rules are <10 fields
				l = strings.ReplaceAll(l, to, "varx")
				l = strings.ReplaceAll(l, from, to)
				l = strings.ReplaceAll(l, "varx", from)
				l = strings.ReplaceAll(l, "append(ret", "append("+to)
				r.body[i] = l
			}
			r.calcUsed()
		}
	}
	//copy(d.flat[recStart:recEnd], newRec)
	return nil
}

func (r *rule) copy() *rule {
	nr := new(rule)
	nr.fields = make([]string, len(r.fields))
	nr.body = make([]string, len(r.body))
	copy(nr.fields, r.fields)
	copy(nr.body, r.body)
	nr.set = true
	nr.usedVars = r.usedVars
	nr.name = r.name
	return nr
}

func (d *rulePrefix) addPrefixPartition(name string, rules []*rule) error {
	newRules := make([]*rule, len(rules))
	for i, r := range rules {
		newRules[i] = r.copy()
	}
	p := new(rulePrefix)
	p.prefix = rules[0].fields[0]
	p.flat = newRules
	for _, r := range p.flat {
		r.fields = r.fields[1:]
	}
	if err := p.partition(""); err != nil {
		return err
	}
	if p.prefix == name {
		d.rec = p
	} else {
		d.pref = append(d.pref, p)
	}
	return nil
}

func (d *rulePrefix) partition(name string) error {
	d.calcUsed()

	if len(d.flat) == 1 {
		r := d.flat[0]
		if len(r.fields) == 0 || len(r.fields) == 1 && r.fields[0] == "/*empty*/" {
			r.fields = nil
			d.empty = r
			return nil
		}
	}

	sort.Slice(d.flat, func(i, j int) bool {
		return d.flat[i].name < d.flat[j].name
	})

	var acc []*rule
	for _, r := range d.flat {
		if len(r.fields) == 1 && r.fields[0] == "/*empty*/" {
			r.fields = nil
		}
		if len(r.fields) == 0 {
			// empty rule is special, checked last
			d.empty = r
			continue
		}

		if len(acc) == 0 {
			acc = append(acc, r)
			continue
		}

		match := acc[0].fields[0] == r.fields[0]
		if !match {
			if len(acc) == 1 && acc[0].fields[0] != name {
				d.term = append(d.term, acc[0])
			} else {
				if err := d.addPrefixPartition(name, acc); err != nil {
					return err
				}
			}
			acc = acc[:0]
		}
		acc = append(acc, r)
	}
	if len(acc) == 1 && acc[0].fields[0] != name {
		d.term = append(d.term, acc[0])
	} else if len(acc) > 0 {
		if err := d.addPrefixPartition(name, acc); err != nil {
			return err
		}
	}

	return nil
}

func parseRuleName(s string) string {
	s = strings.TrimSpace(s)
	if s[0] == '|' {
		s = s[1:]
	}
	s = strings.TrimSpace(s)
	return s
}

func parseGoTypes(in []string) ([]goType, error) {
	var ret []goType
	for _, typ := range in {
		parts := strings.Fields(typ)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid go type: %s", typ)
		}
		ret = append(ret, goType{name: parts[0], typ: parts[1]})
	}
	return ret, nil
}

func parseYaccType(in string) ([]yaccType, error) {
	parts := strings.Split(in, " ")
	if parts[0] != "%type" || len(parts) < 3 {
		return nil, fmt.Errorf("invalid yacc type: %s", in)
	}
	typ := parts[1]
	if typ[0] != '<' || typ[len(typ)-1] != '>' {
		return nil, fmt.Errorf("invalid yacc type: %s", in)
	}
	typ = typ[1 : len(typ)-1]
	var ret []yaccType
	for _, p := range parts[2:] {
		ret = append(ret, yaccType{name: p, typ: typ})
	}
	return ret, nil
}

var variableRe = regexp.MustCompile("(New\\w*\\()*\\$([1-6]+[0-9]*|[1-9])")

func normalizeBodyLine(r string) (string, int64, error) {
	r = strings.ReplaceAll(r, "%", "%%")
	r = strings.ReplaceAll(r, "$$ =", "ret =")
	r = strings.ReplaceAll(r, "return 1", "return ret, false")

	var variables int64
	r = strings.ReplaceAll(r, "$$", "ret")
	match := variableRe.FindAllStringSubmatchIndex(r, 1)
	for len(match) > 0 {
		m := match[0]
		start, end := m[4], m[5]

		_int64, err := strconv.ParseInt(r[start:end], 10, 64)
		if err != nil {
			return "", 0, fmt.Errorf("failed to parse variable string: %s", r[start:end])
		}
		if _int64 >= 64 {
			return "", 0, fmt.Errorf("variable reference too big: %d", _int64)
		}
		r = r[:start-1] + "var" + r[start:]
		variables |= (1 << _int64)
		match = variableRe.FindAllStringSubmatchIndex(r, 1)
	}

	//r = strings.ReplaceAll(r, "NewIntVal(var", "NewIntVal(tok")
	//r = strings.ReplaceAll(r, "NewStrVal(var", "NewStrVal(tok")
	//r = strings.ReplaceAll(r, "NewValArg(var", "NewValArg(tok")
	//r = strings.ReplaceAll(r, "NewListArg(var", "NewListArg(tok")
	//r = strings.ReplaceAll(r, "NewBitVal(var", "NewBitVal(tok")
	//r = strings.ReplaceAll(r, "NewHexVal(var", "NewHexVal(tok")

	//r = strings.ReplaceAll(r, "$1", "var1")
	//r = strings.ReplaceAll(r, "$2", "var2")
	//r = strings.ReplaceAll(r, "$3", "var3")
	//r = strings.ReplaceAll(r, "$4", "var4")
	//r = strings.ReplaceAll(r, "$5", "var5")
	//r = strings.ReplaceAll(r, "$6", "var6")
	//r = strings.ReplaceAll(r, "$7", "var7")
	//r = strings.ReplaceAll(r, "$8", "var8")
	//r = strings.ReplaceAll(r, "$9", "var9")
	return r, variables, nil
}
