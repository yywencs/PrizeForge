-- Redis-first 抽奖上线迁移；在每个业务分库执行。
-- 上线前应先停止旧入口，并确认没有 create/fail 状态的 save_order_record 遗留任务；
-- 历史 completed 任务保留，不需要删除。

ALTER TABLE `user_raffle_order_000` DROP COLUMN `account_sync_state`;
ALTER TABLE `user_raffle_order_001` DROP COLUMN `account_sync_state`;
ALTER TABLE `user_raffle_order_002` DROP COLUMN `account_sync_state`;
ALTER TABLE `user_raffle_order_003` DROP COLUMN `account_sync_state`;
