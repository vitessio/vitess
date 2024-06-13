CREATE TABLE `contacts_test`
(
    `id`    bigint      NOT NULL,
    `phone` varchar(16) NOT NULL,
    PRIMARY KEY (`id`)
);

CREATE TABLE `promotion_coupons_test`
(
    `id`         bigint      NOT NULL,
    `code`       varchar(32) NOT NULL,
    `contact_id` bigint unsigned,
    PRIMARY KEY (`id`)
);