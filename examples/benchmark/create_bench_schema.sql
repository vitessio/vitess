create table if not exists bench_orders(
  id bigint not null auto_increment,
  customer_name varchar(255),
  product_sku varchar(128),
  quantity int,
  total_price bigint,
  status varchar(64),
  region varchar(64),
  notes text,
  primary key(id),
  index idx_customer (customer_name),
  index idx_sku_price (product_sku, total_price),
  index idx_status_region (status, region),
  index idx_region_price (region, total_price)
) ENGINE=InnoDB;

create table if not exists bench_events(
  id bigint not null auto_increment,
  event_type varchar(128),
  source varchar(255),
  payload text,
  severity int,
  created_at bigint,
  category varchar(128),
  primary key(id),
  index idx_type_severity (event_type, severity),
  index idx_source (source),
  index idx_category (category),
  index idx_created (created_at)
) ENGINE=InnoDB;

create table if not exists bench_accounts(
  id bigint not null auto_increment,
  username varchar(128),
  email varchar(255),
  balance bigint,
  region varchar(64),
  bio text,
  tier varchar(32),
  primary key(id),
  index idx_username (username),
  index idx_email (email),
  index idx_region_balance (region, balance),
  index idx_tier (tier)
) ENGINE=InnoDB;

create table if not exists bench_logs(
  id bigint not null auto_increment,
  level varchar(32),
  message text,
  component varchar(128),
  error_code int,
  trace_id varchar(64),
  span_id varchar(64),
  primary key(id),
  index idx_level_component (level, component),
  index idx_error_code (error_code),
  index idx_trace (trace_id),
  index idx_span (span_id)
) ENGINE=InnoDB;
