# Advanced Features

The pages below this navigation entry "Advanced Features" can be understood as an addition to the "User Guide". Here we describe advanced Vitess features which you may want to enable or tune in a production setup.

As of October 2017, many of these features are not documented yet. We plan to add pages for them later.

Examples for undocumented features:

* hot row protection in vttablet
* vtgate buffer for lossless failovers
* vttablet consolidator (avoids duplicated read queries to MySQL, turned on by default)
* [vtexplain](https://github.com/youtube/vitess/blob/master/doc/VtExplain.md)
