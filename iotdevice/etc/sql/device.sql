CREATE TABLE `device` (
    `id` bigint NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (`id`),
    `sn` varchar(255) NOT NULL DEFAULT ''
) ENGINE = InnoDB;