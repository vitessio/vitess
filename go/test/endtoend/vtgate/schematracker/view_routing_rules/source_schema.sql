create table t1 (
    id bigint,
    val varchar(100),
    primary key(id)
) Engine=InnoDB;

create view view1 as select id, val from t1;
