CREATE TABLE IF NOT EXISTS _vt.dt_state
(
  dtid varbinary(512) NOT NULL,
  state bigint NOT NULL,
  time_created bigint NOT NULL,
  primary key(dtid)
) ENGINE = InnoDB
