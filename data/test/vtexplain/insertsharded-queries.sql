insert into user (id, name) values(1, "alice");
insert into user (id, name) values(2, "bob");
insert ignore into user (id, name) values(2, "bob");
insert ignore into user (id, name) values(2, "bob"),(3, "charlie");
