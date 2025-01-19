INSERT INTO sales (oid, col1)
 VALUES (1, 'a_1');

INSERT INTO sales_extra(colx, cola, colb, start, end)
VALUES (11, 'a_1', 'b_1',0, 500);

INSERT INTO sales_extra(colx, cola, colb, start, end)
VALUES (12, 'a_2', 'b_2',500, 1000);

INSERT INTO sales_extra(colx, cola, colb, start, end)
VALUES (13, 'a_3', 'b_3',1000, 1500);

INSERT INTO sales_extra(colx, cola, colb, start, end)
VALUES (14, 'a_4', 'b_4',1500, 2000);

INSERT INTO music (id, user_id, col)
VALUES (100, 1, 'foo');

INSERT INTO source_of_ref (id, col, tt)
VALUES (200, 'foo', 2);

INSERT INTO rerouted_ref (id, ref_col, name)
VALUES (200, 'bar', 'baz');