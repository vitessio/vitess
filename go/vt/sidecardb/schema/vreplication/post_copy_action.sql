CREATE TABLE IF NOT EXISTS _vt.post_copy_action(
  id BIGINT NOT NULL auto_increment,
  vrepl_id INT NOT NULL,
  table_name VARBINARY(128) NOT NULL,
  action JSON NOT NULL,
  UNIQUE KEY (vrepl_id, table_name),
  PRIMARY KEY(id)
) ENGINE = InnoDB
