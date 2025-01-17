CREATE TABLE user (
  user_id bigint,
  - name varbinary(128),
  + name varchar(128),
  - email varbinary(128),
  + email varchar(128),
  PRIMARY KEY (user_id)
); 