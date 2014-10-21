# vtocc

vtocc is a smart proxy to mysql/mariadb. It's basically a subset of
vttablet that concerns itself with just serving queries for a
single database. It's a standalone program/server that can be
launched without the need of the rest of the vitess ecosystem.
It can be pointed to an existing database, and you should be able
to send queries through it.
