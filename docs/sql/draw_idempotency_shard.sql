-- Run this migration in every sharded database (for example prizeforge_01 and prizeforge_02).
-- Back up the database and verify index names before applying it to an existing environment.

ALTER TABLE `raffle_activity_account`
  ADD COLUMN `current_order_id` varchar(12) NOT NULL DEFAULT '' COMMENT '当前已扣额度但尚未完成的抽奖订单' AFTER `month_count_surplus`;

ALTER TABLE `user_raffle_order_000`
  ADD COLUMN `request_id` varchar(64) NULL COMMENT '客户端请求幂等ID' AFTER `order_id`,
  ADD COLUMN `draw_state` varchar(16) NOT NULL DEFAULT 'created' COMMENT 'created/processing/success/cancelled' AFTER `order_state`,
  ADD COLUMN `processing_at` datetime NULL COMMENT '抽奖执行权抢占时间' AFTER `draw_state`,
  ADD COLUMN `draw_owner` varchar(32) NOT NULL DEFAULT '' COMMENT '当前抽奖执行者令牌' AFTER `processing_at`;
UPDATE `user_raffle_order_000` SET `request_id` = CONCAT('legacy:', `order_id`) WHERE `request_id` IS NULL;
UPDATE `user_raffle_order_000` SET `draw_state` = 'success' WHERE `order_state` = 'used';
ALTER TABLE `user_raffle_order_000`
  MODIFY COLUMN `request_id` varchar(64) NOT NULL COMMENT '客户端请求幂等ID',
  ADD UNIQUE KEY `uq_user_activity_request` (`user_id`, `activity_id`, `request_id`);

ALTER TABLE `user_raffle_order_001`
  ADD COLUMN `request_id` varchar(64) NULL COMMENT '客户端请求幂等ID' AFTER `order_id`,
  ADD COLUMN `draw_state` varchar(16) NOT NULL DEFAULT 'created' COMMENT 'created/processing/success/cancelled' AFTER `order_state`,
  ADD COLUMN `processing_at` datetime NULL COMMENT '抽奖执行权抢占时间' AFTER `draw_state`,
  ADD COLUMN `draw_owner` varchar(32) NOT NULL DEFAULT '' COMMENT '当前抽奖执行者令牌' AFTER `processing_at`;
UPDATE `user_raffle_order_001` SET `request_id` = CONCAT('legacy:', `order_id`) WHERE `request_id` IS NULL;
UPDATE `user_raffle_order_001` SET `draw_state` = 'success' WHERE `order_state` = 'used';
ALTER TABLE `user_raffle_order_001`
  MODIFY COLUMN `request_id` varchar(64) NOT NULL COMMENT '客户端请求幂等ID',
  ADD UNIQUE KEY `uq_user_activity_request` (`user_id`, `activity_id`, `request_id`);

ALTER TABLE `user_raffle_order_002`
  ADD COLUMN `request_id` varchar(64) NULL COMMENT '客户端请求幂等ID' AFTER `order_id`,
  ADD COLUMN `draw_state` varchar(16) NOT NULL DEFAULT 'created' COMMENT 'created/processing/success/cancelled' AFTER `order_state`,
  ADD COLUMN `processing_at` datetime NULL COMMENT '抽奖执行权抢占时间' AFTER `draw_state`,
  ADD COLUMN `draw_owner` varchar(32) NOT NULL DEFAULT '' COMMENT '当前抽奖执行者令牌' AFTER `processing_at`;
UPDATE `user_raffle_order_002` SET `request_id` = CONCAT('legacy:', `order_id`) WHERE `request_id` IS NULL;
UPDATE `user_raffle_order_002` SET `draw_state` = 'success' WHERE `order_state` = 'used';
ALTER TABLE `user_raffle_order_002`
  MODIFY COLUMN `request_id` varchar(64) NOT NULL COMMENT '客户端请求幂等ID',
  ADD UNIQUE KEY `uq_user_activity_request` (`user_id`, `activity_id`, `request_id`);

ALTER TABLE `user_raffle_order_003`
  ADD COLUMN `request_id` varchar(64) NULL COMMENT '客户端请求幂等ID' AFTER `order_id`,
  ADD COLUMN `draw_state` varchar(16) NOT NULL DEFAULT 'created' COMMENT 'created/processing/success/cancelled' AFTER `order_state`,
  ADD COLUMN `processing_at` datetime NULL COMMENT '抽奖执行权抢占时间' AFTER `draw_state`,
  ADD COLUMN `draw_owner` varchar(32) NOT NULL DEFAULT '' COMMENT '当前抽奖执行者令牌' AFTER `processing_at`;
UPDATE `user_raffle_order_003` SET `request_id` = CONCAT('legacy:', `order_id`) WHERE `request_id` IS NULL;
UPDATE `user_raffle_order_003` SET `draw_state` = 'success' WHERE `order_state` = 'used';
ALTER TABLE `user_raffle_order_003`
  MODIFY COLUMN `request_id` varchar(64) NOT NULL COMMENT '客户端请求幂等ID',
  ADD UNIQUE KEY `uq_user_activity_request` (`user_id`, `activity_id`, `request_id`);

-- The current runtime PO already expects user_id/message_id columns.
-- Add only the uniqueness constraint here after confirming those columns exist.
ALTER TABLE `task`
  ADD UNIQUE KEY `uq_message_id` (`message_id`);
