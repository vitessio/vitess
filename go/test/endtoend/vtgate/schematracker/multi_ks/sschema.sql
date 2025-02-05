create table t(
       id bigint,
       col bigint,
       primary key(id)
) Engine=InnoDB;

create table s(
       id bigint,
       col bigint,
       primary key(id)
) Engine=InnoDB;

create view sv1 as select t.id as id, t.col + s.col as acol from t join s on t.id = s.id;
create view cv1 as select t.id as id, t.col - s.col as scol from t join s on t.id = s.id;
create view cv2 as select t.id as id, t.col * s.col as mcol from t join s on t.id = s.id;
create view cv3 as select t.id as id, t.col / s.col as dcol from t join s on t.id = s.id;