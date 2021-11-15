TYPE=VIEW
query=select `t`.`OBJECT_SCHEMA` AS `object_schema`,`t`.`OBJECT_NAME` AS `object_name`,`t`.`INDEX_NAME` AS `index_name` from (`performance_schema`.`table_io_waits_summary_by_index_usage` `t` join `information_schema`.`statistics` `s` on(((`t`.`OBJECT_SCHEMA` = `s`.`TABLE_SCHEMA`) and (`t`.`OBJECT_NAME` = `s`.`TABLE_NAME`) and (`t`.`INDEX_NAME` = `s`.`INDEX_NAME`)))) where ((`t`.`INDEX_NAME` is not null) and (`t`.`COUNT_STAR` = 0) and (`t`.`OBJECT_SCHEMA` <> \'mysql\') and (`t`.`INDEX_NAME` <> \'PRIMARY\') and (`s`.`NON_UNIQUE` = 1) and (`s`.`SEQ_IN_INDEX` = 1)) order by `t`.`OBJECT_SCHEMA`,`t`.`OBJECT_NAME`
md5=2c155d525f8ab7b1aca079c464d69c64
updatable=1
algorithm=2
definer_user=mysql.sys
definer_host=localhost
suid=0
with_check_option=0
timestamp=2021-11-12 19:51:47
create-version=1
source=SELECT t.object_schema, t.object_name, t.index_name FROM performance_schema.table_io_waits_summary_by_index_usage t INNER JOIN information_schema.statistics s ON t.object_schema = s.table_schema AND t.object_name = s.table_name AND t.index_name = s.index_name WHERE t.index_name IS NOT NULL AND t.count_star = 0 AND t.object_schema != \'mysql\' AND t.index_name != \'PRIMARY\' AND s.NON_UNIQUE = 1 AND s.SEQ_IN_INDEX = 1 ORDER BY object_schema, object_name
client_cs_name=utf8
connection_cl_name=utf8_general_ci
view_body_utf8=select `t`.`OBJECT_SCHEMA` AS `object_schema`,`t`.`OBJECT_NAME` AS `object_name`,`t`.`INDEX_NAME` AS `index_name` from (`performance_schema`.`table_io_waits_summary_by_index_usage` `t` join `information_schema`.`statistics` `s` on(((`t`.`OBJECT_SCHEMA` = `s`.`TABLE_SCHEMA`) and (`t`.`OBJECT_NAME` = `s`.`TABLE_NAME`) and (`t`.`INDEX_NAME` = `s`.`INDEX_NAME`)))) where ((`t`.`INDEX_NAME` is not null) and (`t`.`COUNT_STAR` = 0) and (`t`.`OBJECT_SCHEMA` <> \'mysql\') and (`t`.`INDEX_NAME` <> \'PRIMARY\') and (`s`.`NON_UNIQUE` = 1) and (`s`.`SEQ_IN_INDEX` = 1)) order by `t`.`OBJECT_SCHEMA`,`t`.`OBJECT_NAME`
