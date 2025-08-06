create table customer(
    id bigint,
    region_id smallint,     -- 32=Americas, 96=Europe, 160=APAC, 224=MEA
    country_id smallint,    -- Europe: 1=France, 2=Germany, 3=UK; APAC: 1=India, 2=Japan, 3=Australia; Others: 0
    email varchar(256),
    primary key(id, region_id, country_id)
);