package sqlparser

// Precendence is used to know the precedence between operators,
// so we can introduce parens when needed in the String representation of the AST
type Precendence int

const (
	Syntactic Precendence = iota
	P1
	P2
	P3
	P4
	P5
	P6
	P7
	P8
	P9
	P10
	P11
	P12
	P13
	P14
	P15
	P16
	P17
)

// precedenceFor returns the precedence of an expression.
//
// * NOTE: If you change anything here, update sql.y to keep them consistent.
//   Also make sure to add the new constructs to random_expr.go so we have test coverage for the new expressions *
func precedenceFor(in Expr) Precendence {
	switch node := in.(type) {
	case *OrExpr:
		return P16
	case *XorExpr:
		return P15
	case *AndExpr:
		return P14
	case *NotExpr:
		return P13
	case *BetweenExpr:
		return P12
	case *ComparisonExpr:
		switch node.Operator {
		case EqualOp, NotEqualOp, GreaterThanOp, GreaterEqualOp, LessThanOp, LessEqualOp, LikeOp, InOp, RegexpOp:
			return P11
		}
	case *IsExpr:
		return P11
	case *BinaryExpr:
		switch node.Operator {
		case BitOrOp:
			return P10
		case BitAndOp:
			return P9
		case ShiftLeftOp, ShiftRightOp:
			return P8
		case PlusOp, MinusOp:
			return P7
		case DivOp, MultOp, ModOp, IntDivOp:
			return P6
		case BitXorOp:
			return P5
		}
	case *UnaryExpr:
		switch node.Operator {
		case UPlusOp, UMinusOp:
			return P4
		case BangOp:
			return P3
		case BinaryOp:
			return P2
		}
	case *IntervalExpr:
		return P1
	case *ExtractedSubquery:
		return precedenceFor(node.alternative)
	}

	return Syntactic
}
