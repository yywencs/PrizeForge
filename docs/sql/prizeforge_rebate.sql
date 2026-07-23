-- PrizeForge 返利配置与分表结构。
-- 该脚本在主库及两个分片库初始化之后执行。

CREATE DATABASE IF NOT EXISTS `prizeforge` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
CREATE DATABASE IF NOT EXISTS `prizeforge_01` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
CREATE DATABASE IF NOT EXISTS `prizeforge_02` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

USE `prizeforge`;

CREATE TABLE IF NOT EXISTS `daily_behavior_rebate` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `behavior_type` varchar(32) NOT NULL,
  `rebate_desc` varchar(128) NOT NULL,
  `rebate_type` varchar(32) NOT NULL,
  `rebate_config` varchar(64) NOT NULL,
  `state` varchar(16) NOT NULL DEFAULT 'open',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_behavior_rebate` (`behavior_type`, `rebate_type`, `rebate_config`),
  KEY `idx_behavior_state` (`behavior_type`, `state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `daily_behavior_rebate`
  (`behavior_type`, `rebate_desc`, `rebate_type`, `rebate_config`, `state`)
VALUES
  ('sign', '签到赠送抽奖次数', 'sku', '9011', 'open')
ON DUPLICATE KEY UPDATE
  `rebate_desc` = VALUES(`rebate_desc`),
  `state` = VALUES(`state`);

USE `prizeforge_01`;

CREATE TABLE IF NOT EXISTS `user_behavior_rebate_order_000` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` varchar(32) NOT NULL,
  `order_id` varchar(32) NOT NULL,
  `behavior_type` varchar(32) NOT NULL,
  `out_business_no` varchar(64) NOT NULL,
  `rebate_desc` varchar(128) NOT NULL,
  `rebate_type` varchar(32) NOT NULL,
  `rebate_config` varchar(64) NOT NULL,
  `biz_id` varchar(160) NOT NULL,
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_order_id` (`order_id`),
  UNIQUE KEY `uq_biz_id` (`biz_id`),
  KEY `idx_user_out_business_no` (`user_id`, `out_business_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `user_behavior_rebate_order_001` LIKE `user_behavior_rebate_order_000`;
CREATE TABLE IF NOT EXISTS `user_behavior_rebate_order_002` LIKE `user_behavior_rebate_order_000`;
CREATE TABLE IF NOT EXISTS `user_behavior_rebate_order_003` LIKE `user_behavior_rebate_order_000`;

USE `prizeforge_02`;

CREATE TABLE IF NOT EXISTS `user_behavior_rebate_order_000` LIKE `prizeforge_01`.`user_behavior_rebate_order_000`;
CREATE TABLE IF NOT EXISTS `user_behavior_rebate_order_001` LIKE `prizeforge_01`.`user_behavior_rebate_order_000`;
CREATE TABLE IF NOT EXISTS `user_behavior_rebate_order_002` LIKE `prizeforge_01`.`user_behavior_rebate_order_000`;
CREATE TABLE IF NOT EXISTS `user_behavior_rebate_order_003` LIKE `prizeforge_01`.`user_behavior_rebate_order_000`;
