# ICU data files

These are files copied from the ICU project that contain various types
of data, like character properties.

## How to update

Not all data files are immediately available in the source code, but
need to be built first. This applies to the character / word break
tables.

### Copy from source data

The `icu4c/source/data/in` directory in the source distribution contains
the following ICU data files we use:

```
pnames.icu
ubidi.icu
ucase.icu
unames.icu
ulayout.icu
uprops.icu
nfc.nrm
nfkc.nrm
nfkc_cf.nrm
```

The character and word break table need to be compiled before they can
be copied.

In `icu4c/source` run:

```bash
./configure --with-data-packaging=files
make
```

This will compile the character and word break data into a binary file
that we can use. Once built, the following files we use are available in
`icu4c/source/data/out/build/icudt<XX>l/brkitr`:

```
char.brk
word.brk
```
