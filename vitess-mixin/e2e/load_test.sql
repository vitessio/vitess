-- INSERT TEST DATA
-- mysql --port=15306 --host=127.0.0.1 < load_test.sql
-- SIMULATED QUERIES
-- mysqlslap -c 5 --port=15306 --host=127.0.0.1 --iterations=1000 --create-schema=test_keyspace:80-@primary --query="SELECT * FROM messages;"
-- mysqlslap -c 5 --port=15306 --host=127.0.0.1 --iterations=1000 --create-schema=test_keyspace:80-@replica --query="SELECT * FROM messages;"
-- mysqlslap -c 5 --port=15306 --host=127.0.0.1 --iterations=1000 --create-schema=lookup_keyspace:-@primary --query="SELECT * FROM messages_message_lookup;"
-- mysqlslap -c 5 --port=15306 --host=127.0.0.1 --iterations=1000 --create-schema=lookup_keyspace:-@replica --query="SELECT * FROM messages_message_lookup;"
-- SIMULATED ERRORS
-- ╰─$ mysqlslap --port=15306 --host=127.0.0.1 --iterations=10000 --create-schema=test_keyspace:80-@primary --query="SELECT name FROM messages;"
-- ╰─$ mysqlslap --port=15306 --host=127.0.0.1 --iterations=10000 --create-schema=lookup_keyspace:-@replica --query="SELECT name FROM messages_message_lookup;"

USE test_keyspace:80-@primary; 
INSERT INTO messages (page,time_created_ns,message) VALUES 
(1,1,'test'),
(2,2,'test'),
(3,3,'test'),
(4,4,'test'),
(5,5,'test'),
(6,6,'test'),
(7,7,'test'),
(8,8,'test'),
(9,9,'test');

USE test_keyspace:-80@primary; 
INSERT INTO messages (page,time_created_ns,message) VALUES 
(10,1,'test'),
(11,2,'test'),
(12,3,'test'),
(13,4,'test'),
(14,5,'test'),
(15,6,'test'),
(16,7,'test'),
(17,8,'test'),
(18,9,'test');

USE lookup_keyspace:-@primary; 
INSERT INTO messages_message_lookup (id,page,message) VALUES 
(1,1,'test'),
(2,2,'test'),
(3,3,'test'),
(4,4,'test'),
(5,5,'test'),
(6,6,'test'),
(7,7,'test'),
(8,8,'test'),
(9,9,'test');
