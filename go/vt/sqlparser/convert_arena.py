#!/usr/bin/env python3
"""
Convert sql.y allocation sites to use arena-based allocation.

Converts patterns like:
  $$ = &BinaryExpr{Left: $1, Operator: BitOrOp, Right: $3}
to:
  $$ = yyrcvr.Arena.newBinaryExprV(BinaryExpr{Left: $1, Operator: BitOrOp, Right: $3})

Also converts helper function calls:
  NewStrLiteral($1)      -> newStrLiteralA(yyrcvr.Arena, $1)
  NewIntLiteral($1)      -> newIntLiteralA(yyrcvr.Arena, $1)
  NewSelect(...)         -> newSelectA(yyrcvr.Arena, ...)
  NewWhere(...)          -> newWhereA(yyrcvr.Arena, ...)
  etc.
"""

import re
import sys

# Types that have arena pools (must match Arena struct fields)
POOLED_TYPES = [
    "AliasedExpr", "AliasedTableExpr", "AndExpr", "BinaryExpr", "CaseExpr",
    "ColName", "CollateExpr", "ColumnDefinition", "ComparisonExpr", "ConvertType",
    "CurTimeFuncExpr", "Delete", "DerivedTable", "ExistsExpr", "FuncExpr",
    "GroupConcatExpr", "IndexHint", "IndexOption", "Insert", "IntervalExpr",
    "IsExpr", "JoinCondition", "JoinTableExpr", "Limit", "Literal",
    "MatchExpr", "NotExpr", "NullVal", "Order", "OrExpr",
    "OverClause", "ParenTableExpr", "Select", "SelectInto", "SetExpr",
    "Show", "StarExpr", "Subquery", "TableOption", "UnaryExpr",
    "Union", "UpdateExpr", "Update", "When", "Where", "With", "XorExpr",
]

# Map type name to arena method name (camelCase)
def arena_method(type_name):
    # Convert to camelCase for the method name
    return "new" + type_name + "V"

# Helper functions to convert
HELPER_CONVERSIONS = {
    "NewStrLiteral": "newStrLiteralA",
    "NewIntLiteral": "newIntLiteralA",
    "NewDecimalLiteral": "newDecimalLiteralA",
    "NewFloatLiteral": "newFloatLiteralA",
    "NewHexNumLiteral": "newHexNumLiteralA",
    "NewHexLiteral": "newHexLiteralA",
    "NewBitLiteral": "newBitLiteralA",
    "NewDateLiteral": "newDateLiteralA",
    "NewTimeLiteral": "newTimeLiteralA",
    "NewTimestampLiteral": "newTimestampLiteralA",
    "NewSelect": "newSelectA",
    "NewWhere": "newWhereA",
}

def find_matching_brace(s, start):
    """Find the index of the closing brace matching the opening brace at start."""
    depth = 0
    i = start
    while i < len(s):
        if s[i] == '{':
            depth += 1
        elif s[i] == '}':
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1

def convert_struct_alloc(line, type_name):
    """Convert &TypeName{...} to yyrcvr.Arena.newTypeNameV(TypeName{...})"""
    pattern = "&" + type_name + "{"
    result = line
    offset = 0

    while True:
        idx = result.find(pattern, offset)
        if idx == -1:
            break

        # Find the matching closing brace
        brace_start = idx + len(pattern) - 1  # position of '{'
        brace_end = find_matching_brace(result, brace_start)
        if brace_end == -1:
            # Multi-line allocation, skip
            offset = idx + 1
            continue

        # Extract the struct literal content (including braces)
        struct_content = result[idx + 1:brace_end + 1]  # TypeName{...}

        # Build replacement
        method = arena_method(type_name)
        replacement = f"yyrcvr.Arena.{method}({struct_content})"

        result = result[:idx] + replacement + result[brace_end + 1:]
        offset = idx + len(replacement)

    return result

def convert_helper_call(line, old_name, new_name):
    """Convert OldHelper(args) to newHelper(yyrcvr.Arena, args)"""
    # Pattern: OldHelper( -> newHelper(yyrcvr.Arena,
    # But we need to be careful not to match inside other identifiers
    pattern = re.compile(r'(?<![a-zA-Z0-9_])' + re.escape(old_name) + r'\(')

    def replacer(m):
        return new_name + "(yyrcvr.Arena, "

    return pattern.sub(replacer, line)

def process_file(input_path, output_path):
    with open(input_path, 'r') as f:
        lines = f.readlines()

    converted = 0
    result = []
    in_preamble = False
    past_preamble = False

    for line in lines:
        original = line

        # Don't convert lines in the %{ ... %} preamble (function definitions)
        if '%{' in line:
            in_preamble = True
        if '%}' in line:
            in_preamble = False
            past_preamble = True

        if in_preamble or not past_preamble:
            result.append(line)
            continue

        # Convert struct allocations for each pooled type
        for type_name in POOLED_TYPES:
            if "&" + type_name + "{" in line:
                line = convert_struct_alloc(line, type_name)

        # Convert helper function calls
        for old_name, new_name in HELPER_CONVERSIONS.items():
            if old_name + "(" in line:
                line = convert_helper_call(line, old_name, new_name)

        if line != original:
            converted += 1
        result.append(line)

    with open(output_path, 'w') as f:
        f.writelines(result)

    print(f"Converted {converted} lines", file=sys.stderr)

if __name__ == "__main__":
    input_path = sys.argv[1] if len(sys.argv) > 1 else "sql.y"
    output_path = sys.argv[2] if len(sys.argv) > 2 else input_path
    process_file(input_path, output_path)
