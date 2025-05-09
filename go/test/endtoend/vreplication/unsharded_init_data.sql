insert into customer(cid, name, typ, sport, meta) values(1, 'Jøhn "❤️" Rizzolo',1,'football,baseball','{"industry":"IT SaaS","company":"PlanetScale"}');
insert into customer(cid, name, typ, sport, meta) values(2, 'Paül','soho','cricket',convert(x'7b7d' using utf8mb4));
-- We use a high cid value here to test the target sequence initialization.
insert into customer(cid, name, typ, sport, blb, meta) values(999999, 'ringo','enterprise','','blob data', '{"industry":"Music"}');
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

-- You can see all of the workflow and test failures that this data caused without the fixes
-- from https://github.com/vitessio/vitess/pull/12845 here:
--   https://github.com/vitessio/vitess/pull/12865
insert into vdiff_order VALUES ('PTM1679987542cpodyy4sf09xhwcdt0'),('PTM1679987542cpovyi26kdjr99r9mh'),('PTM1679987542cpoyhemvuqhawgu7h2'),('PTM1679987542cppebyl74z5umuvbgy'),('PTM1679987542cppenm12s7132oix1u'),('PTM1679987542cpprhwddmnlqrmxgv8'),('PTM1679987542cpqccvoplfrtedup38'),('PTM1679987542cpqiy7z5nyiebmgw0b'),('PTM1679987542cpqsmx82uo181zk0hu'),('PTM1679987542cpqw99mrvp5w0zfwze'),('PTM1679987542cprb7l0j5sv31xkdsh'),('PTM1679987542cprnvhdovs3ht3rulz'),('PTM1679987542cprqvrlpsei07bdp0k'),('PTM1679987542cps0c2zhmudrkfc7vq'),('PTM1679987542cps2nj7xcpgrnhrowr'),('PTM1679987542cps2rlvmw652x7fvhi'),('PTM1679987542cps3t9lltjgx561nq2'),('PTM1679987542cps3zgqy64q29f0r2m'),('PTM1679987542cps7r9yf6h2k2bfh4p'),('PTM1679987542cps90m9y34wytn0t50'),('PTM1679987542cpsb601kau592eo52v'),('PTM1679987542cpskzepmf9hs4djvvm'),('PTM1679987542cpsuu774fe3ts74b6k'),('PTM1679987542cpszu54y0ei3iv7cvl'),('PTM1679987542cpt6cljprzj910q37t'),('PTM1679987542cpt7ebkolh2m0w057q'),('PTM1679987542cptav05kc5f183fsbs'),('PTM1679987542cptnln3zpcpfz3n5mr');
insert into vdiff_order VALUES ('fgemkbjemiihhj'),('fgemladahfmfln'),('fgemladcjehjhl'),('fgemmkfkeglelk'),('fgemnbeijlchkd'),('fgenacflnlchlc'),('fgenagakbiknan'),('fgenbemkgckigh'),('fgenbjhkaicaab'),('fgenchmieliead'),('fgencmadlmihhc'),('fgendemknladdb'),('fgendkffcdfckb'),('fgengbdgcdnbke'),('fgenggbbeafdlm'),('fgenhhdgmbacff'),('fgenhkfifnliec'),('fgenidghedhlbi'),('fgeniljmlifbdf'),('fgenjhghnnblek'),('fgenjimadfeada'),('fgenjmibjgbjej'),('fgenkijeghjgjh'),('fgenkjlheiiagc'),('fgenldehanihlh'),('fgenleblfkfidk'),('fgenlfbadakknn'),('fgenlnfmnhahel'),('fgenmaehniienf'),('fgenmdnnjnhaam'),('fgennkilhlchgl'),('fgfaafkmkekinb'),('fgfablecfdbhcg'),('fgfaegmnkalhdb'),('fgfaekdiginclh'),('fgfaencghmcdeg'),('fgfafacfabhldf'),('fgfafcakbfebkh'),('fgfaffifggdmkm'),('fgfafgdghjjbdl'),('fgfagdhififefa'),('fgfagebgahnajh'),('fgfaggabajdjlf'),('fgfahemldkkmkj'),('fgfaiijcagdmhf'),('fgfakdnagaebje'),('fgfakldkjgdeei'),('fgfamdcfejngbl'),('fgfamebkblfkah'),('fgfbbajhmmbfjc'),('fgfbcgfhbkjmkf'),('fgfbcmmmkhkhkd'),('fgfbcmndbmdgdd');
insert into vdiff_order VALUES ('PTM1679907937cpqxpxok13e1l8uhb8'),('PTM1679907937cpr2clvpd8cx8lyeh6'),('PTM1679907937cpr2v5ysgjwxhp0iwj'),('PTM1679907937cpr74yu5duf4ljcwh5'),('PTM1679907937cprdo5jdvlvdjoyz39'),('PTM1679907937cprud30qk89mhyq8sn'),('PTM1679907937cprwj0ib1d2waomzm3'),('PTM1679907937cprxsj9gjvnvonvebg'),('PTM1679907937cprz8t299i57tzowsx'),('PTM1679907937cpswqjwdy610hxn4m8'),('PTM1679907937cptb3o8207kzgy4o7d'),('PTM1679907937cptp2t62nkozfdjun1'),('PTM1679907937cptr3mwkre3uyak3wp'),('PTM1679907937cptsq80wyeckfsisox'),('PTM1679907937cptx6zcxw5c5wksaa5'),('PTM1679907937cpu6t847eclmho0iya'),('PTM1679907937cpuaxr313hudr48pc6'),('PTM1679907937cpuj9tey8be8itwblq'),('PTM1679907937cpuqhc2exc2xmbbdbm'),('PTM1679907937cpvamfjapnz9i7z4dw'),('PTM1679907937cpvkm584ncpu3eznbk'),('PTM1679907937cpvu1quoskgy23pcro'),('PTM1679907937cpwicmfxbbepuro58c'),('PTM1679907937cpwlsdyb0kgsfzl9oh'),('PTM1679907937cpwvzrrwn9l0yhkxeg'),('PTM1679907937cpwxamsgvp23wvm4tw'),('PTM1679907937cpx58xjmmjjzujbjaj'),('PTM1679907937cpx9b61jv3y66fytjw');
insert into vdiff_order VALUES ('icljhkijabacma'),('icljidchfakcbm'),('icljkgccaabhin'),('icljlccinfhjhh'),('icljmkhnjbjeib'),('iclkaajhekmafc'),('iclkafbikjjhbn'),('iclkanegjkjlif'),('iclkbbkjendihg'),('iclkbelchkimhg'),('iclkbnhcediand'),('iclkcbgfkmdagf'),('iclkcggjbanhaj'),('iclkdcdkbdmdji'),('iclkeamemigglm'),('iclkfclhlnbnhn'),('iclkgdlidjafjj'),('iclkgnkhceakgf'),('iclkibamdjmhga'),('iclkihikbnnhel'),('iclkihindgdgdj'),('iclkkagcccniia'),('iclkkfmifacfnn'),('iclkkgaegcbjdn'),('iclkkihhbcjcin'),('iclkmblkmjbhmk'),('iclkmcfeeeilde'),('iclkmdchhmdfmb'),('iclkmdighhgfdj'),('iclkmelendhiih'),('iclkmmbabaadkb'),('icllabaaiieffl'),('icllaclainhmhc'),('icllbajjaknaae'),('icllbhnbbddhem'),('icllbignhmlmmm'),('icllcicgmehhbn'),('iclldbchhkinii'),('icllebcahebbfn'),('icllegcilbhjhl'),('icllemgnfgdmnn'),('icllhkncfglhie'),('icllicmcjgmhhh'),('iclligcijkalhn'),('iclljgndkaljjf'),('iclljijagmjgeb'),('iclljmecicfdki'),('icllkalfkkcfcd'),('icllkfnigibgdj'),('icllklnfcbhgki'),('iclllkhdgcehfg'),('icllnfalfjgnfa'),('iclmbneakkfcdc');
insert into vdiff_order VALUES ('PTM1679898802cpy2sm2f7eeyx1c418'),('PTM1679898802cpy8ilwl38iynquriy'),('PTM1679898802cpydluko9k1ia8soeq'),('PTM1679898802cpyg40s5cle7mq31a0'),('PTM1679898802cpyw1d0vcaqhz13rfv'),('PTM1679898802cpz2a1bg85dsvoe6sd'),('PTM1679898802cpz4525uzoak8cwoob'),('PTM1679898802cpz5cnd2ntkqwhcens'),('PTM1679898802cpzcmtf2x3zg0tl3mq'),('PTM1679898802cpzqxlexdl5ccmswg2'),('PTM1679898802cpzrjs7mexu24zx9j0'),('PTM1679898802cpzvb8eofhntv5zv2w'),('PTM1679898803cp01m9zwvcqel0alnv'),('PTM1679898803cp0h30qplkzbwu47nl'),('PTM1679898803cp0qpuscot9gb98tzx'),('PTM1679898803cp1fyxdh866k0fbp4k'),('PTM1679898803cp1l0powfj4htk4czt'),('PTM1679898803cp2l77jwdrbg7u9fdy'),('PTM1679898803cp2vrcc8bwz9ef8ezu'),('PTM1679898803cp34krilv0gs8made9'),('PTM1679898803cp3agd45d3i69v16mp'),('PTM1679898803cp3ews08395tov755k'),('PTM1679898803cp3jv1qnrtk7v72u2o'),('PTM1679898803cp44d2ks7bfhj955hi'),('PTM1679898803cp4cnjdfc27ut3t2jj'),('PTM1679898803cp4twsyo8w21f14sb0'),('PTM1679898803cp5st72scversqq0z0'),('PTM1679898803cp6ak3ycgwvjj8rawt');

insert into datze(id, dt2, ts1) values (1, '2022-01-01 00:00:00', current_timestamp);
insert into datze(id, dt2, ts1) values (2, '2022-03-27 02:00:00', current_timestamp);
insert into datze(id, dt2, ts1) values (3, '2022-03-27 02:15:00', current_timestamp);
insert into datze(id, dt2, ts1) values (4, '2022-03-27 03:00:00', current_timestamp);
insert into datze(id, dt2, ts1) values (5, '2022-03-27 03:15:00', current_timestamp);
insert into datze(id, dt2, ts1) values (6, current_timestamp, current_timestamp);

insert into geom_tbl(id, g, p, ls, pg, mp, mls, mpg, gc) values(1,ST_GeomFromText("LINESTRING(0 0,1 2,2 4)"), POINT(32767, 12345678901234567890),ST_GeomFromText("LINESTRING(-1 1,32627 32678,32679 65536,1234567890123456789 489749749734.234212908)"),ST_GeomFromText("POLYGON ((1 2, 2 3, 3 4, 1 2))"), ST_GeomFromText("MULTIPOINT(0 0, 15 25, 45 65)"),ST_GeomFromText("MULTILINESTRING((12 12, 22 22), (19 19, 32 18))"),ST_GeomFromText("MULTIPOLYGON(((0 0,11 0,12 11,0 9,0 0)),((3 5,7 4,4 7,7 7,3 5)))"),ST_GeomFromText("GEOMETRYCOLLECTION(POINT(3 2),LINESTRING(0 0,1 3,2 5,3 5,4 7))"));

insert into reftable (id, val1) values (1, 'a');
insert into reftable (id, val1) values (2, 'b');
insert into reftable (id, val1) values (3, 'c');
insert into reftable (id, val1) values (4, 'd');
insert into reftable (id, val1) values (5, 'e');

insert into ukTable(id1, id2, name) values(1, 10, 'a');
insert into ukTable(id1, id2, name) values(2, 20, 'b');
insert into ukTable(id1, id2, name) values(3, 30, 'c');
insert into ukTable(id1, id2, name) values(4, 40, 'd');
insert into ukTable(id1, id2, name) values(5, 50, 'e');


