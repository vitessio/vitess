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
insert into db_order_test (c_uuid, created_at, dstuff, dtstuff, dbstuff, cstuff) values ('b169-3ad23512-b003-22000b029685-14e3', '2018-02-16 02:38:00', 'EKiech)uichoo7aen4AiY6oop9OoKee4', 'ohghua7chi5ahPhie2io?S8Pi9dewed8', 'UGhaedai]Yai0Aithoozah$ph9acahjo', 'kittens');
insert into db_order_test (c_uuid, created_at, dstuff, dtstuff, dbstuff, cstuff) values ('b169-3ad8fc7a-b289-064d3874effb-14e4', '2018-06-13 14:11:07', 'queen4Uv1Eeshieb6rei3ta1cio[qu6o', 'Athahsea2ahD*u0UKai4aj3ogh0Vou7i', 'jeekiithoo>Ngooze1she0yie8Et1Zie', 'kittens');
insert into db_order_test (c_uuid, created_at, dstuff, dtstuff, dbstuff, cstuff) values ('b169-57d445ba-cc91-22000b048bce-14e3', '2018-04-14 23:56:56', 'aqu3aici9Ish3aiC}oo}laasahdaiMah', 'Aa8aeT+ee3hei1quu*aquie4Eexe?i#b', 'doh1ahy@auCie4zie5dod;es8yiekooj', 'kittens');
insert into db_order_test (c_uuid, created_at, dstuff, dtstuff, dbstuff, cstuff) values ('b169,3ad23512-b003-22000b029685-14e3', '2021-09-15 00:52:55', 'uSh6Roo0iuz.ooqu6aedah1ahk8Aing6', 'yooF3aex0aegh1saF$ei&noopiy0ojoh', 'eeg1Yaevil&ae0ooBa$quahu^Gh1meiG', 'dogs');
insert into db_order_test (c_uuid, created_at, dstuff, dtstuff, dbstuff, cstuff) values ('b169-da8df852-9bad-22000b029685-14e3', '2020-05-06 07:22:17', 'Va-Sh^ee]s3ighiel:a)x1goh9aephoM', 'ahmiut7Ieru8Baz3Moothoh8Ilu:thae', 'egheeshiC9euN1et8aeSh0oohu)hoh7e', 'dogs');
insert into db_order_test (c_uuid, created_at, dstuff, dtstuff, dbstuff, cstuff) values ('b169-c15236c2-c126-0a35b5b9cfad-14e4', '2018-11-23 01:25:09', 'wo9ohphah=g7se-uP7chiuf8noochi?G', 'uD9Neesh)eefe0CheeGhetae9aejXoog', 'aiChi3imee{v6aulei6iesh1iTh3hoNe', 'kittens');
insert into db_order_test (c_uuid, created_at, dstuff, dtstuff, dbstuff, cstuff) values ('b169-888dc696-ba7c-1231391275f1-14e2', '2018-10-07 18:52:25', 'ob#iug8zoo2waXa8feu2kaebeebieca5', 'cha8Re]Leepheeme,ihooF2iej5coo7e', 'mi0Eijoh1eiChie7jae4Sha=caeD9nu6', 'dogs');
insert into db_order_test (c_uuid, created_at, dstuff, dtstuff, dbstuff, cstuff) values ('b169-7e6f4774-99e5-22000b010ed3-14e3', '2018-04-02 19:09:00', 'shoo=k3ophujei=fie8ohwahWee!yo3f', 'ashay]equiel7tho7moh5shem5eiD?ah', 'Eewie-sh3xae8iu3ahth3cohxoo1oohe', 'birds');
insert into db_order_test (c_uuid, created_at, dstuff, dtstuff, dbstuff, cstuff) values ('b169-7359cfb5-9ff5-064d3874effb-14e4', '2018-12-11 01:46:46', 'Fohk0aif4oov!e>ith)eeghoo`Goh)p0', 'shi3ahde9doo5Uph6CeiSheCh/uw0nae', 'audaek{eceenooPh8wichahcheiv9thu', 'chickens');
insert into db_order_test (c_uuid, created_at, dstuff, dtstuff, dbstuff, cstuff) values ('b169-a8411858-a983-123139285dbf-14e2', '2020-03-23 04:42:39', 'ooM4pe<chooph2Zoothah>j>eashaeko', 'too$Shei&s2eing3ashoh0Sh9fiey7th', 'Ohx9saf#eiz*echoo0eechSues_u2que', 'dogs');

insert into datze(id, dt2, ts1) values (1, '2022-01-01 00:00:00', current_timestamp);
insert into datze(id, dt2, ts1) values (2, '2022-03-27 02:00:00', current_timestamp);
insert into datze(id, dt2, ts1) values (3, '2022-03-27 02:15:00', current_timestamp);
insert into datze(id, dt2, ts1) values (4, '2022-03-27 03:00:00', current_timestamp);
insert into datze(id, dt2, ts1) values (5, '2022-03-27 03:15:00', current_timestamp);
insert into datze(id, dt2, ts1) values (6, current_timestamp, current_timestamp);

