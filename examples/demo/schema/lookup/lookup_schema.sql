create table user_idx(user_id bigint not null auto_increment, primary key(user_id));
create table name_user_idx(name varchar(128), user_id bigint, primary key(name, user_id));
create table music_user_idx(music_id bigint not null auto_increment, user_id bigint, primary key(music_id));
