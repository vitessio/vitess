insert into customer(cid, name) values(1, 'sougou');
insert into customer(cid, name) values(6, 'demmer');
insert into merchant(mname, category) values('monoprice', 'electronics');
insert into merchant(mname, category) values('newegg', 'electronics');
insert into product(pid, description) values(1, 'keyboard');
insert into product(pid, description) values(2, 'monitor');
insert into orders(oid, cid, mname, pid, price) values(1, 1, 'monoprice', 1, 10);
insert into orders(oid, cid, mname, pid, price) values(2, 1, 'newegg', 2, 15);
insert into orders(oid, cid, mname, pid, price) values(3, 6, 'monoprice', 2, 20);