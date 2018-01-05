
/* this is a comment about a commented query */
/* SELECT * from users; */

/* this is a comment about another commented query */
-- SELECT * from users;

/* this is a comment about a query with a semicolon; or two; */
SELECT * from user;

/* this is a semicolon with no query */
;

-- this is a single line comment at the end of the file

-- semicolon in comment
select /* ; */ 1 from user;

-- semicolon in string
select 1 from user where x=';';

-- comment value in string
select 1 from user where x='/* hello */';

-- comment value with semicolon in string
select 1 from user where x='/* ; */';
