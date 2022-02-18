insert into customer(cid, name, typ, sport, meta) values(1, 'Jøhn "❤️" Rizzolo',1,'football,baseball','{}');
insert into customer(cid, name, typ, sport, meta) values(2, 'Paül','soho','cricket',convert(x'7b7d' using utf8mb4));
insert into customer(cid, name, typ, sport) values(3, 'ringo','enterprise','');
insert into merchant(mname, category) values('Monoprice', 'eléctronics');
insert into merchant(mname, category) values('newegg', 'elec†ronics');
insert into product(pid, description) values(1, 'keyböard ⌨️');
insert into product(pid, description) values(2, 'Monitor 🖥️');
insert into orders(oid, cid, mname, pid, price, qty) values(1, 1, 'monoprice', 1, 10, 1);
insert into orders(oid, cid, mname, pid, price, qty) values(2, 1, 'Newegg', 2, 15, 2);
insert into orders(oid, cid, mname, pid, price, qty) values(3, 2, 'monoprîce', 2, 20, 3);
insert into customer2(cid, name, typ, sport) values(1, 'jo˙n',1,'football,baseball');
insert into customer2(cid, name, typ, sport) values(2, 'Pául','soho','cricket');
insert into customer2(cid, name, typ, sport) values(3, 'Ringo','enterprise','');
-- for testing edge cases:
--   1. where inserted binary value is 15 bytes, field is 16, mysql adds a null while storing but binlog returns 15 bytes
--   2. where mixed case, special characters, or reserved words are used in identifiers
insert into `Lead`(`Lead-id`, name) values (x'02BD00987932461E8820C908E84BAE', 'abc');
insert into mysql_order_test (c_uuid, created_at) values ('b169-3ad23512-b003-22000b029685-14e3', '2018-02-16 02:38:00');
insert into mysql_order_test (c_uuid, created_at) values ('b169-3ad8fc7a-b289-064d3874effb-14e4', '2018-06-13 14:11:07');
insert into mysql_order_test (c_uuid, created_at) values ('b169-57d445ba-cc91-22000b048bce-14e3', '2018-04-14 23:56:56');
insert into mysql_order_test (c_uuid, created_at) values ('b169,3ad23512-b003-22000b029685-14e3', '2021-09-15 00:52:55');
insert into mysql_order_test (c_uuid, created_at) values ('b169-da8df852-9bad-22000b029685-14e3', '2020-05-06 07:22:17');
insert into mysql_order_test (c_uuid, created_at) values ('b169-c15236c2-c126-0a35b5b9cfad-14e4', '2018-11-23 01:25:09');
insert into mysql_order_test (c_uuid, created_at) values ('b169-888dc696-ba7c-1231391275f1-14e2', '2018-10-07 18:52:25');
insert into mysql_order_test (c_uuid, created_at) values ('b169-7e6f4774-99e5-22000b010ed3-14e3', '2018-04-02 19:09:00');
insert into mysql_order_test (c_uuid, created_at) values ('b169-7359cfb5-9ff5-064d3874effb-14e4', '2018-12-11 01:46:46');
insert into mysql_order_test (c_uuid, created_at) values ('b169-a8411858-a983-123139285dbf-14e2', '2020-03-23 04:42:39');

