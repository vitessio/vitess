package vtgate

import (
	"net/url"
	"strings"

	pbvtgate "github.com/planetscale/psevents/go/vtgate/v1"
)

// splitComments splits a SQL string, optionally containing /**/-style comments, into
// a block of non-comments SQL and an array of 0 or more comment sections.  The
// returned comment strings do not include the /**/ markers.
func splitComments(sql string) (string, []string) {
	sb := strings.Builder{}
	var comments []string
	var tok []string
	for tok = strings.SplitN(sql, "/*", 2); len(tok) == 2; tok = strings.SplitN(sql, "/*", 2) {
		tok[0] = strings.TrimSpace(tok[0])
		if sb.Len() > 0 && len(tok[0]) > 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString(tok[0])

		tok = strings.SplitN(tok[1], "*/", 2)
		comments = append(comments, strings.TrimSpace(tok[0]))
		if len(tok) == 2 {
			sql = tok[1]
		} else {
			sql = ""
		}
	}

	tok[0] = strings.TrimSpace(tok[0])
	if sb.Len() > 0 && len(tok[0]) > 0 {
		sb.WriteByte(' ')
	}
	sb.WriteString(tok[0])

	return sb.String(), comments
}

// parseCommentTags parses sqlcommenter-style key-value pairs out of an array of comments,
// e.g. as returned by splitComments.
// The sqlcommenter format is loosely this:
//   /*key='value',key2='url%2dencoded value',key3='I\'m using back\\slashes'*/
// The full spec is here: https://google.github.io/sqlcommenter/spec/
func parseCommentTags(comments []string) []*pbvtgate.Query_Tag {
	var ret []*pbvtgate.Query_Tag
	for _, block := range comments {
		if strings.HasPrefix(block, "+") {
			// It's a query optimizer hint, a la sqlparser.queryOptimizerPrefix
			continue
		}
		for {
			var key, value, encKey, encValue string
			tok := strings.SplitN(block, "=", 2)
			if len(tok) < 2 {
				break
			}

			encKey, block = strings.TrimSpace(tok[0]), strings.TrimSpace(tok[1])
			key, err := url.QueryUnescape(encKey)
			if err != nil {
				key = encKey
			}

			ok, encValue, nextBlock := splitQuotedValue(block)
			if !ok {
				break
			}
			value, err = url.QueryUnescape(encValue)
			if err != nil {
				value = encValue
			}
			ret = append(ret, &pbvtgate.Query_Tag{Key: key, Value: value})
			block = strings.TrimSpace(nextBlock)
			if strings.HasPrefix(block, ",") {
				block = strings.TrimSpace(strings.Trim(block, ","))
			} else {
				break
			}
		}
	}

	return ret
}

// splitQuotedValue splits whatever comes after the '=' sign, looking for
// a value in single quotes.  It expands any '\' escape sequences.  A sample
// valid input might look like: `'isn\'t',next='great'`, for which the function
// returns (true, "isn't", ",next='great'").
func splitQuotedValue(block string) (ok bool, value string, remainder string) {
	// must start with a single quote
	if len(block) == 0 || block[0] != '\'' {
		return false, "", ""
	}

	// fast path: no escape characters
	tok := strings.SplitN(block, "'", 3)
	// invariant: tok[0]==""
	if len(tok) < 3 {
		return false, "", ""
	}
	if !strings.Contains(tok[1], `\`) {
		return true, tok[1], tok[2]
	}

	// invariant: block has at least two `'` characters
	// invariant: block has at least one `\` character

	// harder path, contains escape characters.  We need to reparse, because what we
	// thought was the the ending "'" character might be escaped.
	sb := strings.Builder{}
	left := 1
	// loop invariant: len(block) >= left
	for idx := strings.IndexAny(block[left:], `'\`); idx != -1; idx = strings.IndexAny(block[left:], `'\`) {
		// invariant: len(block) > left+idx
		// invariant: block[left+idx] is either `'` or `\`
		sb.WriteString(block[left : left+idx])
		if block[left+idx] == '\'' {
			// ending single quote; we're done
			return true, sb.String(), block[left+idx+1:]
		}
		// invariant: block[left+idx] == `\`
		if len(block) == left+idx+1 {
			// block ends with a backslash, naughty!
			return false, "", ""
		}
		// invariant: len(block) > left+idx+1
		// All escapes do is protect the literal character afterwards.  In this world, `\n` just means `n`.
		sb.WriteByte(block[left+idx+1])
		left = left + idx + 2
	}

	// invariant: block[left:] has no `'` or `\` characters
	// block ended without a terminating single quote
	return false, "", ""
}
