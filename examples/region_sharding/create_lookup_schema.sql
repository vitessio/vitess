CREATE TABLE IF NOT EXISTS customer_lookup (
  id int NOT NULL,
  keyspace_id varbinary(128),
  primary key(id)
  );
