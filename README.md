[![Build Status](https://travis-ci.org/youtube/vitess.svg?branch=master)](https://travis-ci.org/youtube/vitess/builds)
[![codebeat badge](https://codebeat.co/badges/51c9a056-1103-4522-9a9c-dc623821ea87)](https://codebeat.co/projects/github-com-youtube-vitess)
[![Go Report Card](https://goreportcard.com/badge/github.com/youtube/vitess)](https://goreportcard.com/report/github.com/youtube/vitess)

# Vitess 

Vitess is a database clustering system for horizontal scaling of MySQL
through generalized sharding.

By encapsulating shard-routing logic, Vitess allows application code and
database queries to remain agnostic to the distribution of data onto
multiple shards. With Vitess, you can even split and merge shards as your needs
grow, with an atomic cutover step that takes only a few seconds.

Vitess has been a core component of YouTube's database infrastructure
since 2011, and has grown to encompass tens of thousands of MySQL nodes.

For more about Vitess, please visit [vitess.io](http://vitess.io).

## Contact

Ask questions in the
[vitess@googlegroups.com](https://groups.google.com/forum/#!forum/vitess)
discussion forum or on [Gitter](https://gitter.im/youtube/vitess).

Subscribe to
[vitess-announce@googlegroups.com](https://groups.google.com/forum/#!forum/vitess-announce)
or the [Vitess Blog](http://blog.vitess.io/)
for low-frequency updates like new features and releases.

## License

Unless otherwise noted, the Vitess source files are distributed
under the BSD-style license found in the LICENSE file.
