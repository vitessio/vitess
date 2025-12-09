# Replica Warming Feature

When brand new replicas are brought into service, they have empty buffer pools. For large and busy databases, the time it takes to get the buffer pool correctly full can take multiple minutes, and during that time period they serve queries significantly slower - to the point it can be detrimental to the application.

Outside of Vitess, a common way of solving this problem is to "warm" replicas - send a small amount of production traffic to them, enough that the overall application remains in SLA, but enough that the right data ends up in their buffer pool. This process generally takes between 5 and 30 minutes, and is crucial.

In PlanetScale, one of the special ways that we deploy Vitess involves a lot of brand new node creation - because we leverage instance storage, every time we deploy a new machine (for a customer resizing a database, or recycling a node for security reasons) it comes up on a brand new instance, and needs to be warmed.

We need to solve this problem.

## Replica Warming

Vitess' replica distribution logic is not wildly complicated. If you look at the "balancers" (go/vt/vtgate/balancer/balancer.go) there's some that distribute queries completely randomly, some that have some "flow" based for even distribution, and one that just uses tablets from inside of the zone.

There are lots of general ways to solve this, but I want to start with a very simple and specific solution. What PlanetScale does when we get a brand new replica restore from backup is "surge" - if we have one replica per zone, we create a second replica in that zone, restore it and catch it up, and then turn off the old replica when it's done. If we have two replicas per zone, we bring up two, etc.

What I am going to have us do now is when we "surge", we leave the old replicas around for a fixed amount of time (let's say 30 minutes), which will give us time to send the majority of reads to them, and then warm the replica up.

What I want to configure VTGate with now is the ability to understand when there are older replicas and brand new replicas, and be able to prefer the newer replicas for a certain amount of time. Basically, if we have new (< 30 minute) old replicas, and older replicas, we should warm up the new replicas until they're considered "aged"

## Challenges

With this, something we do not want to sacrifice is overloading replicas because VTTablet has restarted, or something, and then we end up thinking that a replica is not warm, and overloading the existing replicas. I'm not sure how to solve this problem immediately, but it is a concern.

## Things to do

I do not believe VTGate knows the age of a VTTablet, so we are going to have to teach Vitess how to track when a VTTablet was started, and plumb this all the way through to where we get the VTTablet information.

Then, we are going to need to add a new balancer type that accomplishes our goal.

## Your mission

OK Claude, your job is to read my plan and ask me questions. Before we write a line of code, I want you to hone my thinking - we need to build a system that is reliable and resilient, but also performant.
