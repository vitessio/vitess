CREATE TABLE customer (
  id int NOT NULL,
  fullname varbinary(256),
  nationalid varbinary(256),
  country varbinary(256),
  primary key(id)
  );
CREATE TABLE customer_lookup (
  id int NOT NULL,
  keyspace_id varbinary(256),
  primary key(id)
  );
