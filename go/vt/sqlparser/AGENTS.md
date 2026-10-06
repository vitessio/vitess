# SQL Parser

## Tokenizer

The tokenizer (`token.go`) processes SQL input character-by-character. When
modifying scan methods, watch out for **recursion and stack overflow**. Scan
helper methods must not call back into `Scan()`. Crafted input with many
consecutive constructs can cause unbounded stack growth. Instead, have helpers
advance scanner state and return to `Scan`'s main `for` loop via `continue`.
