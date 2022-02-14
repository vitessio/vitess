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


