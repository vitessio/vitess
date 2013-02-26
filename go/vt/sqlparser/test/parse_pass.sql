select 1 from t
select -1 from t where b = -2
select /* simplest */ 1 from t
select /* back-quote */ 1 from `t`#select /* back-quote */ 1 from t
select /* back-quote keyword */ 1 from `from`#select /* back-quote keyword */ 1 from `from`
select /* @ */ @@a from b
select /* \0 */ '\0' from a
select 1 /* drop this comment */ from t#select 1 from t
select /* union */ 1 from t union select 1 from t
select /* union all */ 1 from t union all select 1 from t
select /* minus */ 1 from t minus select 1 from t
select /* except */ 1 from t except select 1 from t
select /* intersect */ 1 from t intersect select 1 from t
select /* distinct */ distinct 1 from t
select /* for update */ 1 from t for update
select /* select list */ 1, 2 from t
select /* * */ * from t
select /* column alias */ a b from t#select /* column alias */ a as b from t
select /* column alias with as */ a as b from t
select /* a.* */ a.* from t
select /* select with bool expr */ a = b from t
select /* case_when */ case when a = b then c end from t
select /* case_when_else */ case when a = b then c else d end from t
select /* case_when_when_else */ case when a = b then c when b = d then d else d end from t
select /* case */ case aa when a = b then c end from t
select /* parenthesis */ 1 from (t)
select /* table list */ 1 from t1, t2
select /* use */ 1 from t1 use index (a) where b = 1
select /* use */ 1 from t1 as t2 use index (a), t3 use index (b) where b = 1
select /* table alias */ 1 from t t1#select /* table alias */ 1 from t as t1
select /* table alias with as */ 1 from t as t1
select /* join */ 1 from t1 join t2
select /* straight_join */ 1 from t1 straight_join t2
select /* left join */ 1 from t1 left join t2
select /* left outer join */ 1 from t1 left outer join t2#select /* left outer join */ 1 from t1 left join t2
select /* right join */ 1 from t1 right join t2
select /* right outer join */ 1 from t1 right outer join t2#select /* right outer join */ 1 from t1 right join t2
select /* inner join */ 1 from t1 inner join t2#select /* inner join */ 1 from t1 join t2
select /* cross join */ 1 from t1 cross join t2
select /* natural join */ 1 from t1 natural join t2
select /* join on */ 1 from t1 join t2 on a = b
select /* s.t */ 1 from s.t
select /* select in from */ 1 from (select 1 from t)
select /* where */ 1 from t where a = b
select /* and */ 1 from t where a = b and a = c
select /* or */ 1 from t where a = b or a = c
select /* not */ 1 from t where not a = b
select /* exists */ 1 from t where exists (select 1 from t)
select /* (boolean) */ 1 from t where not (a = b)
select /* in value list */ 1 from t where a in (b, c)
select /* in select */ 1 from t where a in (select 1 from t)
select /* not in */ 1 from t where a not in (b, c)
select /* like */ 1 from t where a like b
select /* not like */ 1 from t where a not like b
select /* between */ 1 from t where a between b and c
select /* not between */ 1 from t where a not between b and c
select /* is null */ 1 from t where a is null
select /* is not null */ 1 from t where a is not null
select /* < */ 1 from t where a < b
select /* <= */ 1 from t where a <= b
select /* >= */ 1 from t where a >= b
select /* <> */ 1 from t where a <> b
select /* <=> */ 1 from t where a <=> b
select /* != */ 1 from t where a != b
select /* single value expre list */ 1 from t where a in (b)
select /* select as a value expression */ 1 from t where a = (select a from t)
select /* parenthesised value */ 1 from t where a = (b)#select /* parenthesised value */ 1 from t where a = b
select /* over-parenthesize */ ((1)) from t where ((a)) in (((1))) and ((a, b)) in ((((1,1))), ((2,2)))#select /* over-parenthesize */ 1 from t where a in (1) and (a, b) in ((1, 1), (2, 2))
select /* dot-parenthesize */ (a.b) from t where (b.c) = 2#select /* dot-parenthesize */ a.b from t where b.c = 2
select /* & */ 1 from t where a = b&c
select /* | */ 1 from t where a = b|c
select /* ^ */ 1 from t where a = b^c
select /* + */ 1 from t where a = b+c
select /* - */ 1 from t where a = b-c
select /* * */ 1 from t where a = b*c
select /* / */ 1 from t where a = b/c
select /* % */ 1 from t where a = b%c
select /* u+ */ 1 from t where a = +b
select /* u- */ 1 from t where a = -b
select /* u~ */ 1 from t where a = ~b
select /* empty function */ 1 from t where a = b()
select /* function with 1 param */ 1 from t where a = b(c)
select /* function with many params */ 1 from t where a = b(c, d)
select /* if as func */ 1 from t where a = if(b)
select /* function with distinct */ count(distinct a) from t
select /* a */ a from t
select /* a.b */ a.b from t
select /* string */ 'a' from t
select /* double quoted string */ "a" from t#select /* double quoted string */ 'a' from t
select /* quote quote in string */ 'a''a' from t#select /* quote quote in string */ 'a\'a' from t
select /* double quote quote in string */ "a""a" from t#select /* double quote quote in string */ 'a\"a' from t
select /* quote in double quoted string */ "a'a" from t#select /* quote in double quoted string */ 'a\'a' from t
select /* backslash quote in string */ 'a\'a' from t
select /* literal backslash in string */ 'a\\na' from t
select /* all escapes */ '\0\'\"\b\n\r\t\Z\\' from t
select /* non-escape */ '\x' from t#select /* non-escape */ 'x' from t
select /* unescaped backslash */ '\n' from t
select /* value argument */ :a from t
select /* value argument with dot */ :a.b from t
select /* null */ null from t
select /* octal */ 010 from t
select /* hex */ 0xf0 from t
select /* float */ 0.1 from t
select /* group by */ 1 from t group by a
select /* having */ 1 from t having a = b
select /* simple order by */ 1 from t order by a#select /* simple order by */ 1 from t order by a asc
select /* order by asc */ 1 from t order by a asc
select /* order by desc */ 1 from t order by a desc
select /* limit a */ 1 from t limit a
select /* limit a,b */ 1 from t limit a, b
insert /* simple */ into a values (1)
insert /* multi-value */ into a values (1, 2)
insert /* multi-value list */ into a values (1, 2), (3, 4)
insert /* value expression list */ into a values (a+1, 2*3)
insert /* column list */ into a(a, b) values (1, 2)
insert /* select */ into a select b, c from d
insert /* on duplicate */ into a values (1, 2) on duplicate key update b = values(a), c = d
update /* simple */ a set b = 3
update /* list */ a set b = 3, c = 4
update /* expression */ a set b = 3+4
update /* where */ a set b = 3 where a = b
update /* order */ a set b = 3 order by c desc
update /* limit */ a set b = 3 limit c
delete /* simple */ from a
delete /* where */ from a where a = b
delete /* order */ from a order by b desc
delete /* limit */ from a limit b
set /* simple */ a = 3
set /* list */ a = 3, b = 4
alter ignore table a add foo#alter table a
alter table a add foo#alter table a
alter table a alter foo#alter table a
alter table a change foo#alter table a
alter table a modify foo#alter table a
alter table a drop foo#alter table a
alter table a disable foo#alter table a
alter table a enable foo#alter table a
alter table a order foo#alter table a
alter table a default foo#alter table a
alter table a discard foo#alter table a
alter table a import foo#alter table a
alter table a rename b#rename table a b
alter table a rename to b#rename table a b
create table a
create table if not exists a#create table a
create index a on b#alter table b
create unique index a on b#alter table b
create unique index a using foo on b#alter table b
drop table a
drop table if exists a#drop table a
drop index b on a#alter table a
