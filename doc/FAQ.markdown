# Vitess Frequently Asked Questions

## Is Vitess used at Google?

Yes, Vitess has been used to serve all YouTube database traffic since
2011.

## Why not do it inside MySQL?

MySQL is really good at what it does: serving queries with a low
latency, on a single server. However, it was not designed to do a lot
of the things at the scale that we need at YouTube. Including the
changes we needed inside MySQL itself would have been a huge
project. Additionally, it’s a large C++ codebase that’s almost 20
years old, which means that making changes is more complicated than we
would like. We decided to correct or enforce the behaviors we
needed from the outside.

## Why is it written in Go?

Go is a language that hits the sweet spot in terms of expressiveness
and performance: it’s almost as expressive as Python, very
maintainable, but its performance is in the same range as Java (close
to C++ in certain cases). What is more, the language is extremely well
suited for concurrent programming, and it has a very high quality
standard library.

## Why not use X (where X is usually a NoSQL database) instead of putting effort into scaling MySQL?

Every database has its performance profile, meaning that some types of
queries usually run faster than other. Because of that, migrating from
one datastore to another involves a lot more work than just copying
the data and writing equivalent queries. If you need high performance,
you need to rethink how your app accesses its data. MySQL’s low
latency is very hard to match. Moreover, even if it has some quirks,
those quirks are usually pretty well understood.

## What’s up with the name, Vitess?

Vitess was originally named *Voltron*, but we decided to change it
before making it Open Source to avoid infringing any
trademarks. Still, we wanted a name that would allow us to keep using
the prefix VT in our code. *Vitesse* means speed in French – and we
dropped the final E to make it easier to find using Google (also, it
looks much cooler without the E!).

## Why are using you ZooKeeper instead of Etcd/Doozer/Consul/etc?

The topology is a crucial part of our infrastructure, so we didn’t
want to take any risks with it. ZooKeeper may have some rough edges
and not be very easy to set up, but it’s battle tested, so we know we
can rely on it. Of course, you are free to write a plugin to use your
lock service of choice – we would appreciate if you contributed the
code back.

## Is the Vitess used at Google the same as the Open Source version?

Mostly. The core functionality remains unchanged, but we add some code
to take advantage of Google’s infrastructure (so we can use Chubby,
the Protocol Buffers based RPC system, etc.). Our philosophy is “Open
Source first” – when we develop a new feature, we first make it work
in the Open Source tree, and only then write a plugin that makes use
of Google specific technologies. We believe this is very important to
keep us honest, and to ensure that the Open Source version of Vitess
is as high quality as the internal one. The vast majority of of our
development takes place in the open, on GitHub. This also means that
Vitess is built with extensibility in mind – it should be pretty
straightforward to adjust it to the needs of your infrastructure.
