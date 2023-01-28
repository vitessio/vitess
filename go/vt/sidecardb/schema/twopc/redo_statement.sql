CREATE TABLE IF NOT EXISTS _vt.redo_statement(
  dtid varbinary(512) NOT NULL,
  id bigint NOT NULL,
  statement mediumblob NOT NULL,
  primary key(dtid, id)
) ENGINE = InnoDB
