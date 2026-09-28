CREATE TABLE t1
(
    id         BIGINT PRIMARY KEY,
    name       VARCHAR(255) NOT NULL,
    age        INT,
    email      VARCHAR(100),
    created_at DATETIME,
    is_active  BOOLEAN,
    INDEX age_idx (age)
);