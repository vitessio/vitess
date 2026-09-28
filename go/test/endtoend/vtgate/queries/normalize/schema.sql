create table t1(
                   id bigint unsigned not null,
                   charcol char(10),
                   vcharcol varchar(50),
                   bincol binary(50),
                   varbincol varbinary(50),
                   floatcol float,
                   deccol decimal(5,2),
                   bitcol bit,
                   datecol date,
                   enumcol enum('small', 'medium', 'large'),
                   setcol set('a', 'b', 'c'),
                   jsoncol json,
                   geocol geometry,
                   binvalcol varbinary(50),
                   binnumcol varbinary(50),
                   primary key(id)
) Engine=InnoDB;