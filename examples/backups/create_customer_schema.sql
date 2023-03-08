create table if not exists customer(
    customer_id bigint not null,
    email varbinary(128),
    primary key(customer_id)
) ENGINE=InnoDB;

create table if not exists corder(
    order_id bigint not null,
    customer_id bigint,
    sku varbinary(128),
    price bigint,
    primary key(order_id)
) ENGINE=InnoDB;
