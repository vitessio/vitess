# Contributing to Vitess

You want to contribute to Vitess? That's awesome!

We're looking forward to any contribution! Before you start larger contributions, make sure to reach out first and discuss your plans with us.

This page describes for new contributors how to make yourself familiar with Vitess and the programming language Go.

## Learning Go

Vitess was one of the early adaptors of [Google's programming language Go](https://golang.org/).

We love it for its simplicity (e.g. compared to C++ or Java) and performance (e.g. compared to Python).

Contributing to our server code will require you to learn Go. We recommend to read the following resources.

### Go Tour

https://tour.golang.org/

The Go tour is a browser based tutorial which explains the different concepts of the programming language.
It's interactive i.e. you can change and run all examples on the right side.
The later steps also have specific exercises which you're supposed to implement yourself.
It's a lot of fun and demonstrates how simple it is to write Go code.

### Go Readability

While there's no Go style guide, there is a set of recommendations in the Go community which add up to an implicit style guide.
To make sure you're writing idiomatic Go code, please read the following documents:

* Go Readability slides: https://talks.golang.org/2014/readability.slide
  * Talk about Go readability with many specific examples.
* `Effective Go`: https://golang.org/doc/effective_go.html
  * Recommendations for writing good Go code.
* Go Code Review Comments: https://github.com/golang/go/wiki/CodeReviewComments 
  * The closest thing to a style guide.

### Other Resources

If you're unsure about Go's behavior or syntax, we recommend to look it up in the specification: https://golang.org/ref/spec
It is well written and easy to understand.

### Appreciating Go

After using Go for several weeks, we hope that you'll start to love Go as much as we do. 

In our opinion, the song "Write in Go" from ScaleAbility, a Google acapella band, perfectly captures what's so special about Go. Watch it and enjoy that you learnt Go: www.youtube.com/watch?v=LJvEIjRBSDA

## Learning Vitess

Vitess is a complex distributed system. There are a few design docs in the `/doc` section. The best way to ramp up on vitess is by starting to use it.
Then, you can dive into the code to see how the various parts work. For questions, the best place to get them answered is by asking on the slack channel.
You can sign up to the channel by clicking on the top right link at vitess.io.
