set @@sql_mode = '';
insert into customer(cid, name, typ, sport, meta) values(1, 'john',1,'football,baseball','{}');
insert into customer(cid, name, typ, sport, meta) values(2, 'paul','soho','cricket',convert(x'7b7d' using utf8mb4));
insert into customer(cid, name, typ, sport) values(3, 'ringo','enterprise','');
insert into merchant(mname, category) values('monoprice', 'electronics');
insert into merchant(mname, category) values('newegg', 'electronics');
insert into product(pid, description) values(1, 'keyboard');
insert into product(pid, description) values(2, 'monitor');
insert into orders(oid, cid, mname, pid, price, qty) values(1, 1, 'monoprice', 1, 10, 1);
insert into orders(oid, cid, mname, pid, price, qty) values(2, 1, 'newegg', 2, 15, 2);
insert into orders(oid, cid, mname, pid, price, qty) values(3, 2, 'monoprice', 2, 20, 3);
insert into customer2(cid, name, typ, sport) values(1, 'john',1,'football,baseball');
insert into customer2(cid, name, typ, sport) values(2, 'paul','soho','cricket');
insert into customer2(cid, name, typ, sport) values(3, 'ringo','enterprise','');
-- for testing edge cases:
--   1. where inserted binary value is 15 bytes, field is 16, mysql adds a null while storing but binlog returns 15 bytes
--   2. where mixed case, special characters, or reserved words are used in identifiers
insert into `Lead`(`Lead-id`, name) values (x'02BD00987932461E8820C908E84BAE', 'eee');
insert into `Lead`(`Lead-id`, name) values (x'02BD00987932461E8820C908E84BAF', 'fff');
insert into `Lead`(`Lead-id`, name) values (x'02BD00987932461E8820C908E84BAD', 'ddd');
insert into `Lead`(`Lead-id`, name) values (x'02BD00987932461E8820C908E84BAC', 'ccc');
insert into `Lead-1` select * from `Lead`;

