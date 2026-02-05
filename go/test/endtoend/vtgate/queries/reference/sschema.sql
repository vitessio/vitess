CREATE TABLE IF NOT EXISTS delivery_failure (
    id BIGINT NOT NULL,
    zip_detail_id BIGINT NOT NULL,
    reason VARCHAR(255),
    PRIMARY KEY(id)
) ENGINE=InnoDB;