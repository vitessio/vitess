// Package messages defines a pubsub style of interfacing with the Vitess Messaging feature.
// The goal is to provide a simple and performant way to use message queues without needing
// to worry about the underlying SQL and system architecture. All operations are safe
// for concurrent use.
//
// More details can be found at http://vitess.io/advanced/messaging/
//
// Queues
//
// Queues are the core component of messaging, and require the MySQL tables to be
// set up before they can be used. They are treated like any other table in terms of
// vschema and sharding. The definition requires strict field ordering for required
// fields, followed by N payload fields. There is no restriction on the payload field types.
//
//		create table my_important_messages(
//			-- required fields for Vitess - order matters
//			time_scheduled bigint,
//			id bigint,
//			time_next bigint,
//			epoch bigint,
//			time_created bigint,
//			time_acked bigint,
//
//			-- whatever payload fields your queue needs, including potential sharding keys
//			my_data_field_1 varchar(128),
//			my_data_field_2 bigint,
//			my_data_field_3 json,
//
//			-- recommended indexes for common Vitess queries
//			primary key(time_scheduled, id),
//			unique index id_idx(id),
//			index next_idx(time_next, epoch)
//
//			-- Vitess specific settings are set in the comments
//		) comment 'vitess_message,vt_ack_wait=30,vt_purge_after=86400,vt_batch_size=10,vt_cache_size=10000,vt_poller_interval=30'
//
// Inserting and Acknowledging Messages
//
// Perhaps the most valuable feature of Vitess messages is that you can atomically
// add, ack or fail messages in the same transaction as your actual data commits.
// The Execer interface lets you optionally provide a sql.DB / sql.Tx that these commands
// will be executed on. There is no requirement that these need to use the Vitess driver.
// You can perform any of these operations without needing to run Queue.Open.
//
// Subscribing to Messages
//
// This utilizes some custom Vitess syntax and connection parameters, so the queue
// itself creates a sql.DB that it uses under the hood to stream messages. The max concurrency
// definition determines how many messages will be leased out and kept in memory at any given
// time. No connection is made until Queue.Open is run.
package messages
