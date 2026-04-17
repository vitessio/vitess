create table t1
(
    id  bigint,
    col bigint,
    primary key (id)
) Engine = InnoDB;

create table t2
(
    id    bigint,
    col   bigint,
    mycol varchar(50),
    primary key (id),
    index(id, mycol),
    index(id, col),
    foreign key (id) references t1 (id) on delete restrict
) Engine = InnoDB;

create table t3
(
    id  bigint,
    col bigint,
    primary key (id),
    foreign key (col) references t1 (id) on delete restrict
) Engine = InnoDB;

create table multicol_tbl1
(
    cola bigint,
    colb varbinary(50),
    colc varchar(50),
    msg  varchar(50),
    primary key (cola, colb, colc)
) Engine = InnoDB;

create table multicol_tbl2
(
    cola bigint,
    colb varbinary(50),
    colc varchar(50),
    msg  varchar(50),
    primary key (cola, colb, colc),
    foreign key (cola, colb, colc) references multicol_tbl1 (cola, colb, colc) on delete cascade
) Engine = InnoDB;

create table t4
(
    id       bigint,
    col      bigint,
    t2_mycol varchar(50),
    t2_col   bigint,
    primary key (id),
    foreign key (id) references t2 (id) on delete restrict,
    foreign key (id, t2_mycol) references t2 (id, mycol) on update restrict,
    foreign key (id, t2_col) references t2 (id, col) on update cascade
) Engine = InnoDB;

create table t5
(
    pk   bigint,
    sk   bigint,
    col1 varchar(50),
    primary key (pk),
    index(sk, col1)
) Engine = InnoDB;

create table t6
(
    pk   bigint,
    sk   bigint,
    col1 varchar(50),
    primary key (pk),
    foreign key (sk, col1) references t5 (sk, col1) on delete restrict on update restrict
) Engine = InnoDB;

create table u_t1
(
    id bigint,
    col1 bigint,
    index(col1),
    primary key (id)
) Engine = InnoDB;

create table u_t2
(
    id bigint,
    col2 bigint,
    primary key (id),
    foreign key (col2) references u_t1 (col1) on delete set null on update set null
) Engine = InnoDB;

create table u_t3
(
    id bigint,
    col3 bigint,
    primary key (id),
    foreign key (col3) references u_t1 (col1) on delete cascade on update cascade
) Engine = InnoDB;


/*
 *                    fk_t1
 *                        │
 *                        │ On Delete Restrict
 *                        │ On Update Restrict
 *                        ▼
 *   ┌────────────────fk_t2────────────────┐
 *   │                                     │
 *   │On Delete Set Null                   │ On Delete Set Null
 *   │On Update Set Null                   │ On Update Set Null
 *   ▼                                     ▼
 * fk_t7                                fk_t3───────────────────┐
 *                                         │                    │
 *                                         │                    │ On Delete Set Null
 *                      On Delete Set Null │                    │ On Update Set Null
 *                      On Update Set Null │                    │
 *                                         ▼                    ▼
 *                                      fk_t4                fk_t6
 *                                         │
 *                                         │
 *                      On Delete Restrict │
 *                      On Update Restrict │
 *                                         │
 *                                         ▼
 *                                      fk_t5
 */

create table fk_t1
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col)
) Engine = InnoDB;

create table fk_t2
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t1(col) on delete restrict on update restrict
) Engine = InnoDB;

create table fk_t3
(
    id bigint,
    col varchar(10),
    primary key (id),
    unique index(col),
    foreign key (col) references fk_t2(col) on delete set null on update set null
) Engine = InnoDB;

create table fk_t4
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t3(col) on delete set null on update set null
) Engine = InnoDB;

create table fk_t5
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t4(col) on delete restrict on update restrict
) Engine = InnoDB;

create table fk_t6
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t3(col) on delete set null on update set null
) Engine = InnoDB;

create table fk_t7
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t2(col) on delete set null on update set null
) Engine = InnoDB;

/*
 *                fk_t10
 *                   │
 * On Delete Cascade │
 * On Update Cascade │
 *                   │
 *                   ▼
 *                fk_t11──────────────────┐
 *                   │                    │
 *                   │                    │ On Delete Restrict
 * On Delete Cascade │                    │ On Update Restrict
 * On Update Cascade │                    │
 *                   │                    │
 *                   ▼                    ▼
 *                fk_t12               fk_t13
 */

create table fk_t10
(
    id bigint,
    col varchar(10),
    primary key (id),
    unique index(col)
) Engine = InnoDB;

create table fk_t11
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t10(col) on delete cascade on update cascade
) Engine = InnoDB;

create table fk_t12
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t11(col) on delete cascade on update cascade
) Engine = InnoDB;

create table fk_t13
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t11(col) on delete restrict on update restrict
) Engine = InnoDB;

/*
 *                 fk_t15
 *                    │
 *                    │
 *  On Delete Cascade │
 *  On Update Cascade │
 *                    │
 *                    ▼
 *                 fk_t16
 *                    │
 * On Delete Set Null │
 * On Update Set Null │
 *                    │
 *                    ▼
 *                 fk_t17──────────────────┐
 *                    │                    │
 *                    │                    │ On Delete Set Null
 *  On Delete Cascade │                    │ On Update Set Null
 *  On Update Cascade │                    │
 *                    │                    │
 *                    ▼                    ▼
 *                 fk_t18               fk_t19
 */

create table fk_t15
(
    id bigint,
    col varchar(10),
    primary key (id),
    unique index(col)
) Engine = InnoDB;

create table fk_t16
(
    id bigint,
    col varchar(10),
    primary key (id),
    unique index(col),
    foreign key (col) references fk_t15(col) on delete cascade on update cascade
) Engine = InnoDB;

create table fk_t17
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t16(col) on delete set null on update set null
) Engine = InnoDB;

create table fk_t18
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t17(col) on delete cascade on update cascade
) Engine = InnoDB;

create table fk_t19
(
    id bigint,
    col varchar(10),
    primary key (id),
    index(col),
    foreign key (col) references fk_t17(col) on delete set null on update set null
) Engine = InnoDB;

/*
    Self referenced foreign key from col2 to col in fk_t20
*/

create table fk_t20
(
    id bigint,
    col varchar(10),
    col2 varchar(10),
    primary key (id),
    index(col),
    foreign key (col2) references fk_t20(col) on delete restrict on update restrict
) Engine = InnoDB;


/*
 *              fk_multicol_t1
 *                        │
 *                        │ On Delete Restrict
 *                        │ On Update Restrict
 *                        ▼
 *   ┌────────fk_multicol_t2───────────────┐
 *   │                                     │
 *   │On Delete Set Null                   │ On Delete Set Null
 *   │On Update Set Null                   │ On Update Set Null
 *   ▼                                     ▼
 * fk_multicol_t7              fk_multicol_t3───────────────────┐
 *                                         │                    │
 *                                         │                    │ On Delete Set Null
 *                      On Delete Set Null │                    │ On Update Set Null
 *                      On Update Set Null │                    │
 *                                         ▼                    ▼
 *                             fk_multicol_t4       fk_multicol_t6
 *                                         │
 *                                         │
 *                      On Delete Restrict │
 *                      On Update Restrict │
 *                                         │
 *                                         ▼
 *                             fk_multicol_t5
 */
create table fk_multicol_t1
(
    id bigint,
    colb varchar(10),
    cola varchar(10),
    primary key (id),
    index(cola, colb),
    unique index(colb)
) Engine = InnoDB;

create table fk_multicol_t2
(
    id bigint,
    colb varchar(10) default 'xyz',
    cola varchar(10),
    primary key (id),
    unique index(cola, colb),
    foreign key (cola, colb) references fk_multicol_t1(cola, colb) on delete restrict on update restrict
) Engine = InnoDB;

create table fk_multicol_t3
(
    id bigint,
    colb varchar(10),
    cola varchar(10),
    primary key (id),
    index(cola, colb),
    foreign key (cola, colb) references fk_multicol_t2(cola, colb) on delete set null on update set null
) Engine = InnoDB;

create table fk_multicol_t4
(
    id bigint,
    colb varchar(10),
    cola varchar(10) default 'abcd',
    primary key (id),
    index(cola, colb),
    unique index(cola),
    foreign key (cola, colb) references fk_multicol_t3(cola, colb) on delete set null on update set null
) Engine = InnoDB;

create table fk_multicol_t5
(
    id bigint,
    colb varchar(10),
    cola varchar(10),
    primary key (id),
    index(cola, colb),
    foreign key (cola, colb) references fk_multicol_t4(cola, colb) on delete restrict on update restrict
) Engine = InnoDB;

create table fk_multicol_t6
(
    id bigint,
    colb varchar(10),
    cola varchar(10),
    primary key (id),
    index(cola, colb),
    foreign key (cola, colb) references fk_multicol_t3(cola, colb) on delete set null on update set null
) Engine = InnoDB;

create table fk_multicol_t7
(
    id bigint,
    colb varchar(10),
    cola varchar(10),
    primary key (id),
    index(cola, colb),
    foreign key (cola, colb) references fk_multicol_t2(cola, colb) on delete set null on update set null
) Engine = InnoDB;

/*
 *       fk_multicol_t10
 *                   │
 * On Delete Cascade │
 * On Update Cascade │
 *                   │
 *                   ▼
 *       fk_multicol_t11──────────────────┐
 *                   │                    │
 *                   │                    │ On Delete Restrict
 * On Delete Cascade │                    │ On Update Restrict
 * On Update Cascade │                    │
 *                   │                    │
 *                   ▼                    ▼
 *       fk_multicol_t12      fk_multicol_t13
 */

create table fk_multicol_t10
(
    id bigint,
    colb varchar(10),
    cola varchar(10),
    primary key (id),
    index(cola, colb)
) Engine = InnoDB;

create table fk_multicol_t11
(
    id bigint,
    colb varchar(10),
    cola varchar(10),
    primary key (id),
    index(cola, colb),
    foreign key (cola, colb) references fk_multicol_t10(cola, colb) on delete cascade on update cascade
) Engine = InnoDB;

create table fk_multicol_t12
(
    id bigint,
    colb varchar(10),
    cola varchar(10),
    primary key (id),
    index(cola, colb),
    foreign key (cola, colb) references fk_multicol_t11(cola, colb) on delete cascade on update cascade
) Engine = InnoDB;

create table fk_multicol_t13
(
    id bigint,
    colb varchar(10),
    cola varchar(10),
    primary key (id),
    index(cola, colb),
    foreign key (cola, colb) references fk_multicol_t11(cola, colb) on delete restrict on update restrict
) Engine = InnoDB;

/*
 *        fk_multicol_t15
 *                    │
 *                    │
 *  On Delete Cascade │
 *  On Update Cascade │
 *                    │
 *                    ▼
 *        fk_multicol_t16
 *                    │
 * On Delete Set Null │
 * On Update Set Null │
 *                    │
 *                    ▼
 *        fk_multicol_t17──────────────────┐
 *                    │                    │
 *                    │                    │ On Delete Set Null
 *  On Delete Cascade │                    │ On Update Set Null
 *  On Update Cascade │                    │
 *                    │                    │
 *                    ▼                    ▼
 *        fk_multicol_t18      fk_multicol_t19
 */

create table fk_multicol_t15
(
    id bigint,
    colb varchar(10),
    cola varchar(10),
    primary key (id),
    index(cola, colb)
) Engine = InnoDB;

create table fk_multicol_t16
(
    id bigint,
    colb varchar(10),
    cola varchar(10),
    primary key (id),
    index(cola, colb),
    foreign key (cola, colb) references fk_multicol_t15(cola, colb) on delete cascade on update cascade
) Engine = InnoDB;

create table fk_multicol_t17
(
    id bigint,
    colb varchar(10),
    cola varchar(10),
    primary key (id),
    index(cola, colb),
    foreign key (cola, colb) references fk_multicol_t16(cola, colb) on delete set null on update set null
) Engine = InnoDB;

create table fk_multicol_t18
(
    id bigint,
    colb varchar(10),
    cola varchar(10),
    primary key (id),
    index(cola, colb),
    foreign key (cola, colb) references fk_multicol_t17(cola, colb) on delete cascade on update cascade
) Engine = InnoDB;

create table fk_multicol_t19
(
    id bigint,
    colb varchar(10),
    cola varchar(10),
    primary key (id),
    index(cola, colb),
    foreign key (cola, colb) references fk_multicol_t17(cola, colb) on delete set null on update set null
) Engine = InnoDB;
