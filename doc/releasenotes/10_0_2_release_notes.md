## Bug fixes
### Query Serving
 * Fixes encoding of sql strings #8029
 * Fix for issue with information_schema queries with both table name and schema name predicates #8099
 * PRIMARY in index hint list for release 10.0 #8159
### VReplication
 * VReplication: Pad binlog values for binary() columns to match the value returned by mysql selects #8137
## CI/Build
### Build/CI
 * update release notes with known issue #8081
## Documentation
### Other
 * Post v10.0.1 updates #8045
## Enhancement
### Build/CI
 * Added release script to the makefile #8030
### Other
 * Add optional TLS feature to gRPC servers #8176
## Other
### Build/CI
 * Release 10.0.1 #8031

The release includes 14 commits (excluding merges)
Thanks to all our contributors: @GuptaManan100, @askdba, @deepthi, @harshit-gangal, @noxiouz, @rohit-nayak-ps, @systay
