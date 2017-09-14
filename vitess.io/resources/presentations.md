---
layout: doc
title: "Presentations and Videos"
redirect_from: /resources/presentations.html
description: Slides and videos from presentations about Vitess.
modified:
excerpt:
tags: []
image:
  feature:
  teaser:
  thumb:
toc: true
share: false
---

## Percona Live 2016

[Sugu](https://github.com/sougou) and [Anthony](https://github.com/enisoc)
showed what it looks like to use Vitess now that
[Keyspace IDs]({% link overview/concepts.md %}#keyspace-id) can be
completely hidden from the application. They gave a live demo of
[resharding the Guestbook sample app]({% link user-guide/sharding-kubernetes.md %}),
which now knows nothing about shards, and explained how new features in VTGate
make all of this possible.

<iframe src="http://docs.google.com/gview?url=http://vitess.io/resources/percona-2016.pdf&embedded=true" style="width:643px; height:379px; margin-top: 20px;" frameborder="0"></iframe>
<div style="text-align: right; width: 100%; margin-bottom:20px"><a href="http://vitess.io/resources/percona-2016.pdf">download slides</a></div>

## CoreOS Meetup, January 2016

Vitess team member [Anthony Yeh](https://github.com/enisoc)'s talk at
the [January 2016 CoreOS Meetup](http://www.meetup.com/coreos/events/228233948/)
discussed challenges and techniques for running distributed databases
within Kubernetes, followed by a deep dive into the design trade-offs
of the [Vitess on Kubernetes](https://github.com/youtube/vitess/tree/master/examples/kubernetes)
deployment templates.

<iframe src="http://docs.google.com/gview?url=http://vitess.io/resources/coreos-meetup-2016-01-27.pdf&embedded=true" style="width:643px; height:379px; margin-top: 20px;" frameborder="0"></iframe>
<div style="text-align: right; width: 100%; margin-bottom:20px"><a href="http://vitess.io/resources/coreos-meetup-2016-01-27.pdf">download slides</a></div>

## Oracle OpenWorld 2015

Vitess team member [Anthony Yeh](https://github.com/enisoc)'s talk at
Oracle OpenWorld 2015 focused on
what the [Cloud Native Computing](http://cncf.io) paradigm means when
applied to MySQL in the cloud. The talk also included a deep dive into
[transparent, live resharding]
({% link user-guide/sharding.md %}#resharding), one of the key
features of Vitess that makes it well-adapted for a Cloud Native environment.

<iframe src="http://docs.google.com/gview?url=http://vitess.io/resources/openworld-2015-vitess.pdf&embedded=true" style="width:643px; height:379px; margin-top: 20px;" frameborder="0"></iframe>
<div style="text-align: right; width: 100%; margin-bottom:20px"><a href="http://vitess.io/resources/openworld-2015-vitess.pdf">download slides</a></div>

## Percona Live 2015

Vitess team member [Anthony Yeh](https://github.com/enisoc)'s talk at
Percona Live 2015 provided an overview of Vitess as well as an explanation
of how Vitess has evolved to live in a containerized world with
Kubernetes and Docker.

<iframe src="http://docs.google.com/gview?url=http://vitess.io/resources/percona-2015-vitess-and-kubernetes.pdf&embedded=true" style="width:643px; height:379px; margin-top: 20px;" frameborder="0"></iframe>
<div style="text-align: right; width: 100%; margin-bottom:20px"><a href="https://github.com/youtube/vitess/blob/master/doc/slides/Percona2015.pptx?raw=true">download slides</a></div>


## Google I/O 2014 - Scaling with Go: YouTube's Vitess

In this talk, [Sugu Sougoumarane](https://github.com/sougou)
from the Vitess team talks about how Vitess
solved YouTube's scalability problems as well as about tips and techniques
used to scale with Go.<br><br>

<iframe width="640" height="360" src="https://www.youtube.com/embed/midJ6b1LkA0" frameborder="0" allowfullscreen></iframe>
