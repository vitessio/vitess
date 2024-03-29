/*
Copyright 2020 The Vitess Authors.

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

package sqlparser

import (
	"fmt"
	"math/rand/v2"
)

// This file is used to generate random expressions to be used for testing

// Constants for Enum Type - AggregateRule
const (
	CannotAggregate AggregateRule = iota
	CanAggregate
	IsAggregate
)

type (
	ExprGenerator interface {
		Generate(config ExprGeneratorConfig) Expr
	}

	QueryGenerator interface {
		IsQueryGenerator()
		ExprGenerator
	}

	AggregateRule int8

	ExprGeneratorConfig struct {
		// AggrRule determines if the random expression can, cannot, or must be an aggregation expression
		AggrRule AggregateRule
		Type     string
		// MaxCols = 0 indicates no limit
		NumCols int
		// SingleRow indicates that the query must have at most one row
		SingleRow bool
	}

	Generator struct {
		depth          int
		maxDepth       int
		isAggregate    bool
		exprGenerators []ExprGenerator
	}
)

func NewExprGeneratorConfig(aggrRule AggregateRule, typ string, numCols int, singleRow bool) ExprGeneratorConfig {
	return ExprGeneratorConfig{
		AggrRule:  aggrRule,
		Type:      typ,
		NumCols:   numCols,
		SingleRow: singleRow,
	}
}

func (egc ExprGeneratorConfig) SingleRowConfig() ExprGeneratorConfig {
	egc.SingleRow = true
	return egc
}

func (egc ExprGeneratorConfig) MultiRowConfig() ExprGeneratorConfig {
	egc.SingleRow = false
	return egc
}

func (egc ExprGeneratorConfig) SetNumCols(numCols int) ExprGeneratorConfig {
	egc.NumCols = numCols
	return egc
}

func (egc ExprGeneratorConfig) boolTypeConfig() ExprGeneratorConfig {
	egc.Type = "tinyint"
	return egc
}

func (egc ExprGeneratorConfig) intTypeConfig() ExprGeneratorConfig {
	egc.Type = "bigint"
	return egc
}

func (egc ExprGeneratorConfig) stringTypeConfig() ExprGeneratorConfig {
	egc.Type = "varchar"
	return egc
}

func (egc ExprGeneratorConfig) anyTypeConfig() ExprGeneratorConfig {
	egc.Type = ""
	return egc
}

func (egc ExprGeneratorConfig) CannotAggregateConfig() ExprGeneratorConfig {
	egc.AggrRule = CannotAggregate
	return egc
}

func (egc ExprGeneratorConfig) CanAggregateConfig() ExprGeneratorConfig {
	egc.AggrRule = CanAggregate
	return egc
}

func (egc ExprGeneratorConfig) IsAggregateConfig() ExprGeneratorConfig {
	egc.AggrRule = IsAggregate
	return egc
}

func NewGenerator(maxDepth int, exprGenerators ...ExprGenerator) *Generator {
	g := Generator{
		maxDepth:       maxDepth,
		exprGenerators: exprGenerators,
	}
	return &g
}

// enter should be called whenever we are producing an intermediate node. it should be followed by a `defer g.exit()`
func (g *Generator) enter() {
	g.depth++
}

// exit should be called when exiting an intermediate node
func (g *Generator) exit() {
	g.depth--
}

// atMaxDepth returns true if we have reached the maximum allowed depth or the expression tree
func (g *Generator) atMaxDepth() bool {
	return g.depth >= g.maxDepth
}

/*
	 Creates a random expression. It builds an expression tree using the following constructs:
	    - true/false
	    - AND/OR/NOT
	    - string literals, numeric literals (-/+ 1000)
		- columns of types bigint and varchar
		- scalar and tuple subqueries
	    - =, >, <, >=, <=, <=>, !=
		- &, |, ^, +, -, *, /, div, %, <<, >>
	    - IN, BETWEEN and CASE
		- IS NULL, IS NOT NULL, IS TRUE, IS NOT TRUE, IS FALSE, IS NOT FALSE
	Returns the random expression (Expr) and its type (string)

Note: It's important to update this method so that it produces all expressions that need precedence checking.
It's currently missing function calls and string operators
*/
func (g *Generator) Expression(genConfig ExprGeneratorConfig) Expr {
	var options []exprF
	// this will only be used for tuple expressions, everything else will need genConfig.NumCols = 1
	numCols := genConfig.NumCols
	genConfig = genConfig.SetNumCols(1)

	switch genConfig.Type {
	case "bigint":
		options = append(options, func() Expr { return g.intExpr(genConfig) })
	case "varchar":
		options = append(options, func() Expr { return g.stringExpr(genConfig) })
	case "tinyint":
		options = append(options, func() Expr { return g.booleanExpr(genConfig) })
	case "":
		options = append(options, []exprF{
			func() Expr { return g.intExpr(genConfig) },
			func() Expr { return g.stringExpr(genConfig) },
			func() Expr { return g.booleanExpr(genConfig) },
		}...)
	}

	for i := range g.exprGenerators {
		generator := g.exprGenerators[i]
		if generator == nil {
			continue
		}

		// don't create expressions from the expression exprGenerators if we haven't created an aggregation yet
		if _, ok := generator.(QueryGenerator); ok || genConfig.AggrRule != IsAggregate {
			options = append(options, func() Expr {
				expr := generator.Generate(genConfig)
				if expr == nil {
					return g.randomLiteral()
				}
				return expr
			})
		}
	}

	if genConfig.AggrRule != CannotAggregate {
		options = append(options, func() Expr {
			g.isAggregate = true
			return g.randomAggregate(genConfig.CannotAggregateConfig())
		})
	}

	// if an arbitrary number of columns may be generated, randomly choose 1-3 columns
	if numCols == 0 {
		numCols = rand.IntN(3) + 1
	}

	if numCols == 1 {
		return g.makeAggregateIfNecessary(genConfig, g.randomOf(options))
	}

	// with 1/5 probability choose a tuple subquery
	if g.randomBool(0.2) {
		return g.subqueryExpr(genConfig.SetNumCols(numCols))
	}

	tuple := ValTuple{}
	for i := 0; i < numCols; i++ {
		tuple = append(tuple, g.makeAggregateIfNecessary(genConfig, g.randomOf(options)))
	}

	return tuple
}

// makeAggregateIfNecessary is a failsafe to make sure an IsAggregate expression is in fact an aggregation
func (g *Generator) makeAggregateIfNecessary(genConfig ExprGeneratorConfig, expr Expr) Expr {
	// if the generated expression must be an aggregate, and it is not,
	// tack on an extra "and count(*)" to make it aggregate
	if genConfig.AggrRule == IsAggregate && !g.isAggregate && g.depth == 0 {
		expr = &AndExpr{
			Left:  expr,
			Right: &CountStar{},
		}
		g.isAggregate = true
	}

	return expr
}

func (g *Generator) randomAggregate(genConfig ExprGeneratorConfig) Expr {
	isDistinct := rand.IntN(10) < 1

	options := []exprF{
		func() Expr { return &CountStar{} },
		func() Expr { return &Count{Args: Exprs{g.Expression(genConfig.anyTypeConfig())}, Distinct: isDistinct} },
		func() Expr { return &Sum{Arg: g.Expression(genConfig), Distinct: isDistinct} },
		func() Expr { return &Min{Arg: g.Expression(genConfig), Distinct: isDistinct} },
		func() Expr { return &Max{Arg: g.Expression(genConfig), Distinct: isDistinct} },
	}

	g.isAggregate = true
	return g.randomOf(options)
}

func (g *Generator) booleanExpr(genConfig ExprGeneratorConfig) Expr {
	if g.atMaxDepth() {
		return g.booleanLiteral()
	}

	genConfig = genConfig.boolTypeConfig()

	options := []exprF{
		func() Expr { return g.andExpr(genConfig) },
		func() Expr { return g.xorExpr(genConfig) },
		func() Expr { return g.orExpr(genConfig) },
		func() Expr { return g.comparison(genConfig.intTypeConfig()) },
		func() Expr { return g.comparison(genConfig.stringTypeConfig()) },
		//func() Expr { return g.comparison(genConfig) }, // this is not accepted by the parser
		func() Expr { return g.inExpr(genConfig) },
		func() Expr { return g.existsExpr(genConfig) },
		func() Expr { return g.between(genConfig.intTypeConfig()) },
		func() Expr { return g.isExpr(genConfig) },
		func() Expr { return g.notExpr(genConfig) },
		func() Expr { return g.likeExpr(genConfig.stringTypeConfig()) },
	}

	return g.randomOf(options)
}

func (g *Generator) intExpr(genConfig ExprGeneratorConfig) Expr {
	if g.atMaxDepth() {
		return g.intLiteral()
	}

	genConfig = genConfig.intTypeConfig()

	options := []exprF{
		g.intLiteral,
		func() Expr { return g.arithmetic(genConfig) },
		func() Expr { return g.caseExpr(genConfig) },
	}

	return g.randomOf(options)
}

func (g *Generator) stringExpr(genConfig ExprGeneratorConfig) Expr {
	if g.atMaxDepth() {
		return g.stringLiteral()
	}

	genConfig = genConfig.stringTypeConfig()

	options := []exprF{
		g.stringLiteral,
		func() Expr { return g.caseExpr(genConfig) },
	}

	return g.randomOf(options)
}

func (g *Generator) subqueryExpr(genConfig ExprGeneratorConfig) Expr {
	if g.atMaxDepth() {
		return g.makeAggregateIfNecessary(genConfig, g.randomTupleLiteral(genConfig))
	}

	var options []exprF

	for _, generator := range g.exprGenerators {
		if qg, ok := generator.(QueryGenerator); ok {
			options = append(options, func() Expr {
				expr := qg.Generate(genConfig)
				if expr == nil {
					return g.randomTupleLiteral(genConfig)
				}
				return expr
			})
		}
	}

	if len(options) == 0 {
		return g.Expression(genConfig)
	}

	return g.randomOf(options)
}

func (g *Generator) randomTupleLiteral(genConfig ExprGeneratorConfig) Expr {
	if genConfig.NumCols == 0 {
		genConfig.NumCols = rand.IntN(3) + 1
	}

	tuple := ValTuple{}
	for i := 0; i < genConfig.NumCols; i++ {
		tuple = append(tuple, g.randomLiteral())
	}

	return tuple
}

func (g *Generator) randomLiteral() Expr {
	options := []exprF{
		g.intLiteral,
		g.stringLiteral,
		g.booleanLiteral,
	}

	return g.randomOf(options)
}

func (g *Generator) booleanLiteral() Expr {
	return BoolVal(g.randomBool(0.5))
}

// randomBool returns true with probability prob
func (g *Generator) randomBool(prob float32) bool {
	if prob < 0 || prob > 1 {
		prob = 0.5
	}
	return rand.Float32() < prob
}

func (g *Generator) intLiteral() Expr {
	t := fmt.Sprintf("%d", rand.IntN(100)-rand.IntN(100)) //nolint SA4000

	return NewIntLiteral(t)
}

var words = []string{"ox", "ant", "ape", "asp", "bat", "bee", "boa", "bug", "cat", "cod", "cow", "cub", "doe", "dog", "eel", "eft", "elf", "elk", "emu", "ewe", "fly", "fox", "gar", "gnu", "hen", "hog", "imp", "jay", "kid", "kit", "koi", "lab", "man", "owl", "pig", "pug", "pup", "ram", "rat", "ray", "yak", "bass", "bear", "bird", "boar", "buck", "bull", "calf", "chow", "clam", "colt", "crab", "crow", "dane", "deer", "dodo", "dory", "dove", "drum", "duck", "fawn", "fish", "flea", "foal", "fowl", "frog", "gnat", "goat", "grub", "gull", "hare", "hawk", "ibex", "joey", "kite", "kiwi", "lamb", "lark", "lion", "loon", "lynx", "mako", "mink", "mite", "mole", "moth", "mule", "mutt", "newt", "orca", "oryx", "pika", "pony", "puma", "seal", "shad", "slug", "sole", "stag", "stud", "swan", "tahr", "teal", "tick", "toad", "tuna", "wasp", "wolf", "worm", "wren", "yeti", "adder", "akita", "alien", "aphid", "bison", "boxer", "bream", "bunny", "burro", "camel", "chimp", "civet", "cobra", "coral", "corgi", "crane", "dingo", "drake", "eagle", "egret", "filly", "finch", "gator", "gecko", "ghost", "ghoul", "goose", "guppy", "heron", "hippo", "horse", "hound", "husky", "hyena", "koala", "krill", "leech", "lemur", "liger", "llama", "louse", "macaw", "midge", "molly", "moose", "moray", "mouse", "panda", "perch", "prawn", "quail", "racer", "raven", "rhino", "robin", "satyr", "shark", "sheep", "shrew", "skink", "skunk", "sloth", "snail", "snake", "snipe", "squid", "stork", "swift", "swine", "tapir", "tetra", "tiger", "troll", "trout", "viper", "wahoo", "whale", "zebra", "alpaca", "amoeba", "baboon", "badger", "beagle", "bedbug", "beetle", "bengal", "bobcat", "caiman", "cattle", "cicada", "collie", "condor", "cougar", "coyote", "dassie", "donkey", "dragon", "earwig", "falcon", "feline", "ferret", "gannet", "gibbon", "glider", "goblin", "gopher", "grouse", "guinea", "hermit", "hornet", "iguana", "impala", "insect", "jackal", "jaguar", "jennet", "kitten", "kodiak", "lizard", "locust", "maggot", "magpie", "mammal", "mantis", "marlin", "marmot", "marten", "martin", "mayfly", "minnow", "monkey", "mullet", "muskox", "ocelot", "oriole", "osprey", "oyster", "parrot", "pigeon", "piglet", "poodle", "possum", "python", "quagga", "rabbit", "raptor", "rodent", "roughy", "salmon", "sawfly", "serval", "shiner", "shrimp", "spider", "sponge", "tarpon", "thrush", "tomcat", "toucan", "turkey", "turtle", "urchin", "vervet", "walrus", "weasel", "weevil", "wombat", "anchovy", "anemone", "bluejay", "buffalo", "bulldog", "buzzard", "caribou", "catfish", "chamois", "cheetah", "chicken", "chigger", "cowbird", "crappie", "crawdad", "cricket", "dogfish", "dolphin", "firefly", "garfish", "gazelle", "gelding", "giraffe", "gobbler", "gorilla", "goshawk", "grackle", "griffon", "grizzly", "grouper", "haddock", "hagfish", "halibut", "hamster", "herring", "jackass", "javelin", "jawfish", "jaybird", "katydid", "ladybug", "lamprey", "lemming", "leopard", "lioness", "lobster", "macaque", "mallard", "mammoth", "manatee", "mastiff", "meerkat", "mollusk", "monarch", "mongrel", "monitor", "monster", "mudfish", "muskrat", "mustang", "narwhal", "oarfish", "octopus", "opossum", "ostrich", "panther", "peacock", "pegasus", "pelican", "penguin", "phoenix", "piranha", "polecat", "primate", "quetzal", "raccoon", "rattler", "redbird", "redfish", "reptile", "rooster", "sawfish", "sculpin", "seagull", "skylark", "snapper", "spaniel", "sparrow", "sunbeam", "sunbird", "sunfish", "tadpole", "termite", "terrier", "unicorn", "vulture", "wallaby", "walleye", "warthog", "whippet", "wildcat", "aardvark", "airedale", "albacore", "anteater", "antelope", "arachnid", "barnacle", "basilisk", "blowfish", "bluebird", "bluegill", "bonefish", "bullfrog", "cardinal", "chipmunk", "cockatoo", "crayfish", "dinosaur", "doberman", "duckling", "elephant", "escargot", "flamingo", "flounder", "foxhound", "glowworm", "goldfish", "grubworm", "hedgehog", "honeybee", "hookworm", "humpback", "kangaroo", "killdeer", "kingfish", "labrador", "lacewing", "ladybird", "lionfish", "longhorn", "mackerel", "malamute", "marmoset", "mastodon", "moccasin", "mongoose", "monkfish", "mosquito", "pangolin", "parakeet", "pheasant", "pipefish", "platypus", "polliwog", "porpoise", "reindeer", "ringtail", "sailfish", "scorpion", "seahorse", "seasnail", "sheepdog", "shepherd", "silkworm", "squirrel", "stallion", "starfish", "starling", "stingray", "stinkbug", "sturgeon", "terrapin", "titmouse", "tortoise", "treefrog", "werewolf", "woodcock"}

func (g *Generator) stringLiteral() Expr {
	return NewStrLiteral(g.randomOfS(words))
}

func (g *Generator) likeExpr(genConfig ExprGeneratorConfig) Expr {
	g.enter()
	defer g.exit()
	return &ComparisonExpr{
		Operator: LikeOp,
		Left:     g.Expression(genConfig),
		Right:    g.Expression(genConfig),
	}
}

var comparisonOps = []ComparisonExprOperator{EqualOp, LessThanOp, GreaterThanOp, LessEqualOp, GreaterEqualOp, NotEqualOp, NullSafeEqualOp}

func (g *Generator) comparison(genConfig ExprGeneratorConfig) Expr {
	g.enter()
	defer g.exit()

	// specifc 1-3 columns
	numCols := rand.IntN(3) + 1

	cmp := &ComparisonExpr{
		Operator: comparisonOps[rand.IntN(len(comparisonOps))],
		Left:     g.Expression(genConfig.SetNumCols(numCols)),
		Right:    g.Expression(genConfig.SetNumCols(numCols)),
	}
	return cmp
}

func (g *Generator) caseExpr(genConfig ExprGeneratorConfig) Expr {
	g.enter()
	defer g.exit()

	var exp Expr
	var elseExpr Expr
	if g.randomBool(0.5) {
		exp = g.Expression(genConfig.anyTypeConfig())
	}
	if g.randomBool(0.5) {
		elseExpr = g.Expression(genConfig)
	}

	size := rand.IntN(2) + 1
	var whens []*When
	for i := 0; i < size; i++ {
		var cond Expr
		if exp == nil {
			cond = g.Expression(genConfig.boolTypeConfig())
		} else {
			cond = g.Expression(genConfig)
		}

		val := g.Expression(genConfig)
		whens = append(whens, &When{
			Cond: cond,
			Val:  val,
		})
	}

	return &CaseExpr{
		Expr:  exp,
		Whens: whens,
		Else:  elseExpr,
	}
}

var arithmeticOps = []BinaryExprOperator{BitAndOp, BitOrOp, BitXorOp, PlusOp, MinusOp, MultOp, DivOp, IntDivOp, ModOp, ShiftRightOp, ShiftLeftOp}

func (g *Generator) arithmetic(genConfig ExprGeneratorConfig) Expr {
	g.enter()
	defer g.exit()

	op := arithmeticOps[rand.IntN(len(arithmeticOps))]

	return &BinaryExpr{
		Operator: op,
		Left:     g.Expression(genConfig),
		Right:    g.Expression(genConfig),
	}
}

type exprF func() Expr

func (g *Generator) randomOf(options []exprF) Expr {
	return options[rand.IntN(len(options))]()
}

func (g *Generator) randomOfS(options []string) string {
	return options[rand.IntN(len(options))]
}

func (g *Generator) andExpr(genConfig ExprGeneratorConfig) Expr {
	g.enter()
	defer g.exit()
	return &AndExpr{
		Left:  g.Expression(genConfig),
		Right: g.Expression(genConfig),
	}
}

func (g *Generator) orExpr(genConfig ExprGeneratorConfig) Expr {
	g.enter()
	defer g.exit()
	return &OrExpr{
		Left:  g.Expression(genConfig),
		Right: g.Expression(genConfig),
	}
}

func (g *Generator) xorExpr(genConfig ExprGeneratorConfig) Expr {
	g.enter()
	defer g.exit()
	return &XorExpr{
		Left:  g.Expression(genConfig),
		Right: g.Expression(genConfig),
	}
}

func (g *Generator) notExpr(genConfig ExprGeneratorConfig) Expr {
	g.enter()
	defer g.exit()
	return &NotExpr{g.Expression(genConfig)}
}

func (g *Generator) inExpr(genConfig ExprGeneratorConfig) Expr {
	g.enter()
	defer g.exit()

	size := rand.IntN(3) + 2
	inExprGenConfig := NewExprGeneratorConfig(genConfig.AggrRule, "", size, true)
	tuple1 := g.Expression(inExprGenConfig)
	tuple2 := ValTuple{g.Expression(inExprGenConfig)}

	op := InOp
	if g.randomBool(0.5) {
		op = NotInOp
	}

	return &ComparisonExpr{
		Operator: op,
		Left:     tuple1,
		Right:    tuple2,
	}
}

func (g *Generator) between(genConfig ExprGeneratorConfig) Expr {
	g.enter()
	defer g.exit()

	var IsBetween bool
	if g.randomBool(0.5) {
		IsBetween = true
	} else {
		IsBetween = false
	}

	return &BetweenExpr{
		IsBetween: IsBetween,
		Left:      g.Expression(genConfig),
		From:      g.Expression(genConfig),
		To:        g.Expression(genConfig),
	}
}

func (g *Generator) isExpr(genConfig ExprGeneratorConfig) Expr {
	g.enter()
	defer g.exit()

	ops := []IsExprOperator{IsNullOp, IsNotNullOp, IsTrueOp, IsNotTrueOp, IsFalseOp, IsNotFalseOp}

	return &IsExpr{
		Right: ops[rand.IntN(len(ops))],
		Left:  g.Expression(genConfig),
	}
}

func (g *Generator) existsExpr(genConfig ExprGeneratorConfig) Expr {
	expr := g.subqueryExpr(genConfig.MultiRowConfig().SetNumCols(0))
	if subquery, ok := expr.(*Subquery); ok {
		expr = NewExistsExpr(subquery)
	} else {
		// if g.subqueryExpr doesn't return a valid subquery, replace with
		// select 1
		selectExprs := SelectExprs{NewAliasedExpr(NewIntLiteral("1"), "")}
		from := TableExprs{NewAliasedTableExpr(NewTableName("dual"), "")}
		expr = NewExistsExpr(NewSubquery(NewSelect(nil, selectExprs, nil, nil, from, nil, nil, nil, nil)))
	}

	// not exists
	if g.randomBool(0.5) {
		expr = NewNotExpr(expr)
	}

	return expr
}
