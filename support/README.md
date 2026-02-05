# Support for frameworks and tools

This directory includes user contributed notes, scripts , hacks and workarounds for using Vitess
 with common frameworks and Business Intelligence tools.  

It will closely follow https://github.com/vitessio/vitess/issues/5166 but will also focus on finding and highlighting solutions 
that will enable users of these frameworks to use Vitess.

## Objectives

1. For maximum support we should prefer solutions that run queries through vtgate.  
2. To accelerate adoption of Vitess, solutions that do not depend on updating Vitess core are preferred.  
3. If we are not able to achieve the above, we can connect to specific shards in passthrough mode i.e. dbname:- or dbname:-80  

Contributions are very much welcome.