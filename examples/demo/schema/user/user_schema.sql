create table user(user_id bigint, name varchar(128), primary key(user_id));
create table user_extra(user_id bigint, extra varchar(128), primary key(user_id));
create table music(user_id bigint, music_id bigint, primary key(user_id, music_id));
create table music_extra(music_id bigint, keyspace_id bigint unsigned, primary key(music_id));
create table name_info(name varchar(128), info varchar(128), primary key(name));
create table music_user_idx(music_id bigint not null auto_increment, user_id bigint, primary key(music_id));
