# Questions

## Question  1

> Maybe out of topic but what do you think about partition assignment strategy? Do you use this? 
> Particularly, CooperativeStickyAssignor

Kafka partition assignors decide which consumer gets which partitions within a consumer group.

The CooperativeStickyAssignor is an assignor that can be used by the classic consumer group protocol only.
Configured using consumer property: partition.assignment.strategy

You can see what assigner is used when your Spring Kafka application starts. Look for ConsumerConfig values.

partition.assignment.strategy = [class org.apache.kafka.clients.consumer.RangeAssignor, class org.apache.kafka.clients.consumer.CooperativeStickyAssignor]

With CooperativeStickyAssignor, Kafka does not stop the entire consumer group during a rebalance. 
But does a partial stop the world, but some consumers need pause some partitions.

RangeAssignor (eager rebalance):
Complete stop-the-world: Yes
Every consumer in the group stops. All partitions are revoked and reassigned. Full pause.

CooperativeStickyAssignor:
Complete stop-the-world: No, Partial
Only what partitions that needs to move are reassigned. The rest of the consumers keep their partition assigned and keeps consuming.

Long story short: Use CooperativeStickyAssignor
It minimizes downtime, prevents full rebalances, and preserves state better than any eager assignor—including RangeAssignor.

If we compare the new consumer protocol with the old one, the assignment strategy is now controlled by the broker and not by the client.
Rebalances are incremental and much faster especially when your topic has many partitions.

## Question 2

> What happens to the message semantics and acks in terms of shared consumers?

I'm not sure what you mean by "message semantics". Are we talking about message delivery semantics here?
If so, in a shared consumer group the message delivery semantics the default is "at least once".

Also depending on the acknowledgment (implicit, explicit, manual) mode you configure in your Spring Kafka share 
consumer application it's possible the share consumer group will receive the same message more than once. 
For example, if you use manual acknowledgment and your code releases the message.
The maximum number of delivery attempts for a record delivered to a share group is 5 by default, 
but can be configured using the broker configuration: group.share.delivery.count.limit

## Question 3

> Do shared consumers break order of processed messages? 
> In old Kafka, the same key guarantees FIFO. Is this rule broken? 
> Any advice what to do with projects counting on this feature?

The short answer is yes. KIP‑932 (“Queues for Kafka”) breaks Kafka’s classic per‑partition ordering guarantee.
As mention in my talk with the Kafka 4 share consumer protocol, the order of messages is not guaranteed.
So if your consumer application(s) rely on the order of messages, you should not use share consumers!


## Question 4

> The queues work for consumer with subscribe or assign methods?

Share consumer “queues” in KIP‑932 work only with subscribe() they do not work with assign().
But those details are abstracted away by Spring for Apache Kafka.

## Question 5

> If order of events is important, will retriable exceptions help to handle this to consume events by version order, 
> or it will be too expensive and all advantages of Shared consumers will be gone?

If order of events is important, then share consumers are not the right choice regardless on retriable exceptions.
There is no order guarantee at all. For Kafka use case where you rely on strict order of
you messages (only within a single partition) you should use a partition key in combination with regular consumer groups.

## Question 6

> If there are both classic and consumer group configurations, 
> how will the classic consumer move its offset when the new consumer consumes and commits?

Offsets are committed per partition.

When mixing classic and new consumer protocol in the same consumer group (during a live upgrade), 
each partition in the topic is still assigned to exactly one consumer in the consumer group. 
So it's not possible that a partition is assigned to both a classic and new protocol consumer at the same time.
This means that the offsets for a given partition are only committed by either a classic or new protocol. 

The offsets are stored in the `__consumer_offsets` internal topic as usual for and both classic and new consumer 
group protocol.

## 







