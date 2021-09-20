## Bug fixes
 ### Cluster management
  * Port #8422 to 11.0 branch #8744
 ### Query Serving
  * Handle subquery merging with references correctly #8661
  * onlineddl Executor: build schema with DBA user #8667
  * Backport to 11: Fixing a panic in vtgate with OLAP mode #8746
  * Backport into 11: default to primary tablet if not set in VStream api #8766
 ### VReplication
  * Refresh SrvVSchema after an ExternalizeVindex: was missing #8669
 ## CI/Build
 ### Build/CI
  * Vitess  Release 11.0.0 #8549
  * Backport to 11: Updated Makefile do_release script to include godoc steps #8787

 The release includes 18 commits (excluding merges)
 Thanks to all our contributors: @aquarapid, @askdba, @frouioui, @harshit-gangal, @rohit-nayak-ps, @shlomi-noach, @systay
