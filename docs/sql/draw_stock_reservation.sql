-- Run this migration in the database that contains strategy_award.

CREATE TABLE IF NOT EXISTS `strategy_award_stock_reservation` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` varchar(32) NOT NULL COMMENT '用户ID',
  `order_id` varchar(12) NOT NULL COMMENT '抽奖订单ID',
  `strategy_id` bigint NOT NULL COMMENT '策略ID',
  `award_id` bigint NOT NULL COMMENT '奖品ID',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_user_order` (`user_id`, `order_id`),
  KEY `idx_strategy_award` (`strategy_id`, `award_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单维度奖品库存持久化扣减幂等表';
