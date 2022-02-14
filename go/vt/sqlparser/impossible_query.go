package sqlparser

// FormatImpossibleQuery creates an impossible query in a TrackedBuffer.
// An impossible query is a modified version of a query where all selects have where clauses that are
// impossible for mysql to resolve. This is used in the vtgate and vttablet:
//
// - In the vtgate it's used for joins: if the first query returns no result, then vtgate uses the impossible
// query just to fetch field info from vttablet
// - In the vttablet, it's just an optimization: the field info is fetched once form MySQL, cached and reused
// for subsequent queries
func FormatImpossibleQuery(buf *TrackedBuffer, node SQLNode) {
	switch node := node.(type) {
	case *Select:
		buf.Myprintf("select %v from ", node.SelectExprs)
		var prefix string
		for _, n := range node.From {
			buf.Myprintf("%s%v", prefix, n)
			prefix = ", "
		}
		buf.Myprintf(" where 1 != 1")
		if node.GroupBy != nil {
			node.GroupBy.Format(buf)
		}
	case *Union:
		if requiresParen(node.Left) {
			buf.astPrintf(node, "(%v)", node.Left)
		} else {
			buf.astPrintf(node, "%v", node.Left)
		}

		buf.WriteString(" ")
		if node.Distinct {
			buf.WriteString(UnionStr)
		} else {
			buf.WriteString(UnionAllStr)
		}
		buf.WriteString(" ")

		if requiresParen(node.Right) {
			buf.astPrintf(node, "(%v)", node.Right)
		} else {
			buf.astPrintf(node, "%v", node.Right)
		}
	default:
		node.Format(buf)
	}
}
