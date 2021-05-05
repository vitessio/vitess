insert into customer(cid, name, typ, sport, meta) values(1, 'john',1,'football,baseball','{}');
insert into customer(cid, name, typ, sport, meta) values(2, 'paul','soho','cricket',convert(x'7b7d' using utf8mb4));
insert into customer(cid, name, typ, sport) values(3, 'ringo','enterprise','');
insert into merchant(mname, category) values('monoprice', 'electronics');
insert into merchant(mname, category) values('newegg', 'electronics');
insert into product(pid, description) values(1, 'keyboard');
insert into product(pid, description) values(2, 'monitor');
insert into orders(oid, cid, mname, pid, price) values(1, 1, 'monoprice', 1, 10);
insert into orders(oid, cid, mname, pid, price) values(2, 1, 'newegg', 2, 15);
insert into orders(oid, cid, mname, pid, price) values(3, 2, 'monoprice', 2, 20);
insert into customer2(cid, name, typ, sport) values(1, 'john',1,'football,baseball');
insert into customer2(cid, name, typ, sport) values(2, 'paul','soho','cricket');
insert into customer2(cid, name, typ, sport) values(3, 'ringo','enterprise','');
-- for testing edge case where inserted binary value is 15 bytes, field is 16, mysql adds a null while storing but binlog returns 15 bytes
insert into tenant(tenant_id, name) values (x'02BD00987932461E8820C908E84BAE', 'abc');


