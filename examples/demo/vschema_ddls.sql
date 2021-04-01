-- Unsharded Keyspace
alter vschema add table product.product;

-- Sharded Keyspace
alter vschema on customer.customer add vindex hash(customer_id) using hash;

-- Sequences
alter vschema add sequence product.customer_seq;
alter vschema on customer.customer add auto_increment customer_id using product.customer_seq;

-- Shared Vindexes and Foreign Keys
alter vschema on customer.corder add vindex hash(customer_id);
alter vschema add sequence product.corder_seq;
alter vschema on customer.corder add auto_increment corder_id using product.corder_seq;

-- Unique Lookup Vindexes
alter vschema add table product.corder_keyspace_idx;
alter vschema on customer.corder add vindex corder_keyspace_idx(corder_id) using consistent_lookup_unique with owner=`corder`, table=`product.corder_keyspace_idx`, from=`corder_id`, to=`keyspace_id`;

-- Non-Unique Lookup Vindexes
alter vschema on customer.oname_keyspace_idx add vindex unicode_loose_md5(oname) using unicode_loose_md5;
alter vschema on customer.corder add vindex oname_keyspace_idx(oname,corder_id) using consistent_lookup with owner=`corder`, table=`customer.oname_keyspace_idx`, from=`oname,corder_id`, to=`keyspace_id`;

-- Lookup as Primary Vindex
alter vschema add sequence product.corder_event_seq;
alter vschema on customer.corder_event add vindex corder_keyspace_idx(corder_id);
alter vschema on customer.corder_event add auto_increment corder_event_id using product.corder_event_seq;
-- Reversible Vindexes
alter vschema on customer.corder_event add vindex `binary`(keyspace_id) using `binary`;
