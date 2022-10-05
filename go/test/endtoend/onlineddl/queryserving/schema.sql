create table users
(
    id char(36),
    team_id char(36),
    primary key (id)
) ENGINE=InnoDB;

create table music
(
    id char(36),
    music_id char(36),
    primary key (id)
) ENGINE=InnoDB;
