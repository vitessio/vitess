create table t(
       id bigint,
       col bigint,
       primary key(id)
) Engine=InnoDB;

create table u(
       id bigint,
       col bigint,
       primary key(id)
) Engine=InnoDB;

create view uv1 as select t.id as id, t.col + u.col as acol from t join u on t.id = u.id;
create view cv1 as select t.id as id, t.col - u.col as scol from t join u on t.id = u.id;
create view cv2 as select t.id as id, t.col * u.col as mcol from t join u on t.id = u.id;
create view cv3 as select t.id as id, t.col / u.col as dcol from t join u on t.id = u.id;