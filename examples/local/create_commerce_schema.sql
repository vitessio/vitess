create table if not exists product(
  sku VARCHAR(128),
  description VARCHAR(128),
  price bigint,
  primary key(sku)
) ENGINE=InnoDB;
create table if not exists customer(
  customer_id bigint not null auto_increment,
  email VARCHAR(128),
  primary key(customer_id)
) ENGINE=InnoDB;
create table if not exists corder(
  order_id bigint not null auto_increment,
  customer_id bigint,
  sku VARCHAR(128),
  price bigint,
  primary key(order_id)
) ENGINE=InnoDB;
