CREATE TABLE product (
  sku varbinary(128) NOT NULL,
  description varchar(128),
  // ... other columns ...
  PRIMARY KEY (sku)
); 