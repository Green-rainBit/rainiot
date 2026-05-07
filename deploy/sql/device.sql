CREATE TABLE `device` (
    `id` bigint NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (`id`),
    `sn` varchar(255) NOT NULL DEFAULT '',
    UNIQUE KEY `idx_sn` (`sn`)
) ENGINE = InnoDB;