insert into user (id, name) values(1, 'alice');
insert into user (id, name) values(2, 'bob');
insert ignore into user (id, name) values(2, 'bob');
insert ignore into user (id, name, nickname) values(2, 'bob', 'bob');
insert ignore into user (id, name) values(2, 'bob'),(3, 'charlie');
insert into user (id, name, nickname) values(2, 'bob', 'bobby') on duplicate key update nickname='bobby';
insert into user (id, name, nickname, address) values(2, 'bob', 'bobby', '123 main st') on duplicate key update nickname=values(nickname), address=values(address);
insert into user (id, name, nickname, address) values(2, 'bob', 'bobby', '123 main st'), (3, 'jane', 'janie', '456 elm st')on duplicate key update nickname=values(nickname), address=values(address);
