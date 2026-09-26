#!/bin/bash
# Finish addseq after the seqks tablet is visible to vtgate.
V="runuser -u vt -- /home/vt/bin/vtctldclient --server localhost:30021"
for i in $(seq 1 30); do
  mysql -h 127.0.0.1 -P 30003 -u root seqks -e "insert into sbtest_seq (id, next_id, cache) values (0, 1000000000, ${SEQ_CACHE:-1000})" 2>/dev/null && break
  sleep 1
done
$V ApplySchema --sql "CREATE TABLE IF NOT EXISTS sbtest9 (id BIGINT NOT NULL, k INT NOT NULL DEFAULT 0, c CHAR(120) NOT NULL DEFAULT '', pad CHAR(60) NOT NULL DEFAULT '', PRIMARY KEY (id), KEY k_9 (k)) ENGINE=InnoDB" sbtest
vs=$($V GetVSchema sbtest | python3 -c 'import json,sys; v=json.load(sys.stdin); v["tables"]["sbtest9"]={"column_vindexes":[{"column":"id","name":"hash"}],"auto_increment":{"column":"id","sequence":"seqks.sbtest_seq"}}; print(json.dumps(v))')
$V ApplyVSchema --vschema "$vs" sbtest >/dev/null
sleep 2
mysql -h 127.0.0.1 -P 30003 -u root sbtest -e "insert into sbtest9 (k,c,pad) values (1,'a','b'); select id,k from sbtest9; select * from seqks.sbtest_seq"
