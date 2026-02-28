#!/usr/bin/env python3
"""
Replaces t.Errorf / t.Fatalf with testify assert/require in Go test files.

Handles:
  1. Multi-line:
       if err != nil {
           t.Fatalf("...", err)   →   require.NoError(t, err)
       }
  2. Inline if:
       if _, err := foo(); err != nil {
           t.Fatalf("...", err)   →   (leaves if, just replaces inner call)
       }
  3. Bare single-line inside already-open block:
       t.Fatalf("...", err)       →   require.NoError(t, err)

Rules:
  - Inside goroutine (go func): always assert, never require
  - Adds missing imports
  - Matches %v, %s, %w, %q, %d etc.
"""

import re, sys, os

ERR_FORMAT = r'%[vswqd]'  # common format verbs used with err

def goroutine_depth(lines, idx):
    brace_balance = 0
    go_func_starts = []
    for line in lines[:idx]:
        s = line.strip()
        if re.search(r'\bgo\s+func\b', s):
            go_func_starts.append(brace_balance)
        brace_balance += s.count('{') - s.count('}')
        go_func_starts = [x for x in go_func_starts if x < brace_balance]
    return len(go_func_starts)

def has_import(content, pkg):
    return f'"{pkg}"' in content

def add_import(content, pkg):
    m = re.search(r'(import\s*\()(.*?)(\))', content, re.DOTALL)
    if m:
        return content[:m.start()] + m.group(1) + m.group(2) + f'\t"{pkg}"\n' + m.group(3) + content[m.end():]
    m2 = re.search(r'import\s+"([^"]+)"', content)
    if m2:
        return content[:m2.start()] + f'import (\n\t"{m2.group(1)}"\n\t"{pkg}"\n)' + content[m2.end():]
    return content

# Matches: t.Fatalf("...%v...", err)  or  t.Errorf("...%s...", err)
FATALF_ERR = re.compile(r't\.Fatalf\("[^"]*(?:%[vswqd])[^"]*",\s*err\s*\)')
ERRORF_ERR = re.compile(r't\.Errorf\("[^"]*(?:%[vswqd])[^"]*",\s*err\s*\)')

# Matches: if err != nil {    (standalone, nothing else on the line)
IF_ERR_OPEN  = re.compile(r'^(\s*)if err != nil \{\s*$')

def process_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        original = f.read()

    lines = original.splitlines(keepends=True)
    used_assert  = has_import(original, 'github.com/stretchr/testify/assert')
    used_require = has_import(original, 'github.com/stretchr/testify/require')
    need_assert  = False
    need_require = False
    changed = 0

    result_lines = []
    i = 0
    while i < len(lines):
        line  = lines[i]

        # ── 3-line pattern: if err != nil { \n t.Fatal/Errorf \n } ──────────
        m_if = IF_ERR_OPEN.match(line)
        if m_if and i + 2 < len(lines):
            inner   = lines[i + 1].strip()
            closing = lines[i + 2].strip()
            indent  = m_if.group(1)
            in_gor  = goroutine_depth(lines, i) > 0

            if closing == '}':
                if FATALF_ERR.search(inner):
                    fn = 'assert.NoError' if in_gor else 'require.NoError'
                    result_lines.append(f'{indent}{fn}(t, err)\n')
                    need_assert  |= in_gor
                    need_require |= not in_gor
                    changed += 1; i += 3; continue

                if ERRORF_ERR.search(inner):
                    result_lines.append(f'{indent}assert.NoError(t, err)\n')
                    need_assert = True
                    changed += 1; i += 3; continue

        # ── bare single-line t.Fatalf/t.Errorf already inside a block ────────
        indent2 = re.match(r'^(\s*)', line).group(1)
        in_gor2  = goroutine_depth(lines, i) > 0

        if FATALF_ERR.search(line.strip()):
            fn = 'assert.NoError' if in_gor2 else 'require.NoError'
            result_lines.append(f'{indent2}{fn}(t, err)\n')
            need_assert  |= in_gor2
            need_require |= not in_gor2
            changed += 1; i += 1; continue

        if ERRORF_ERR.search(line.strip()):
            result_lines.append(f'{indent2}assert.NoError(t, err)\n')
            need_assert = True
            changed += 1; i += 1; continue

        result_lines.append(line)
        i += 1

    if not changed:
        return False

    content = ''.join(result_lines)
    if need_assert  and not used_assert:
        content = add_import(content, 'github.com/stretchr/testify/assert')
    if need_require and not used_require:
        content = add_import(content, 'github.com/stretchr/testify/require')

    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)

    print(f'  ✓ {path}  ({changed} change(s))')
    return True

def main():
    root = sys.argv[1] if len(sys.argv) > 1 else '.'
    total_files = total_changed = 0
    for dirpath, _, filenames in os.walk(root):
        if any(s in dirpath for s in ['/vendor/', '/node_modules/']):
            continue
        for fname in filenames:
            if not fname.endswith('_test.go'):
                continue
            total_files += 1
            if process_file(os.path.join(dirpath, fname)):
                total_changed += 1
    print(f'\nDone: {total_changed}/{total_files} files changed.')

if __name__ == '__main__':
    main()
