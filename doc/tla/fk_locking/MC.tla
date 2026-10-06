---------------------------------- MODULE MC ----------------------------------
EXTENDS FkLocking

\* The two Vitess cascades on GP race with each other and with two statements
\* on C that Vitess hands to MySQL.
MCWorkload == <<"CascadeUpdGP", "CascadeDelGP", "InsC", "UpdC">>
================================================================================
