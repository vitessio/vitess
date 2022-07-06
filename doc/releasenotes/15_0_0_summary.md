## Major Changes

### Command-line syntax deprecations

### New command line flags and behavior

#### Support for additional compressor and decompressor during backup & restore
We have introduced feature to use your own compressor & decompressor during backup & restore, instead of relying on default compressor. 
There are some built-in compressors which you can use out-of-box instead of default 'pgzip'. We left it to client to evaluate which option
works better for their use-cases. Here are the flags for this feature

- --builtin_compressor
- --builtin_decompressor
- --external_compressor
- --external_decompressor
- --external_compressor_extension
- --compression_level

builtin_compressor as of today support the following compression out-of-box
- pgzip
- pargzip
- lz4
- zstd

If you want to use any of the builtin compressor, simply set one of the above value for "--builtin_compressor". You don't need to set
the builtin_decompressor flag in this case as we infer it automatically from the MANIFEST file. The default value for --builtin_decompressor
is set to "auto".

If you are using custom command or external tool for compression then you need to use "--external_compressor/--external_decompressor" flag.
You need to provide complete command with arguments to these flags. "external_compressor_extension" needs to be set if you are using external
compressor. Leave the value of --builtin_compressor & --builtin-decompressor to their default values if you are using external compressor.
Please note that if you want the current behavior then you don't need to change anything in these flags. You can read more about backup & restore
[here] (https://vitess.io/docs/15.0/user-guides/operating-vitess/backup-and-restore/)

### Online DDL changes