-- 为已存在的 PrizeForge 分库补充 Outbox 扫描索引。
-- 新建数据库已由 prizeforge_01.sql / prizeforge_02.sql 自动创建该索引，
-- 此文件只需要在旧环境执行一次。

ALTER TABLE `prizeforge_01`.`task`
  ADD INDEX `idx_state_update_id` (`state`, `update_time`, `id`);

ALTER TABLE `prizeforge_02`.`task`
  ADD INDEX `idx_state_update_id` (`state`, `update_time`, `id`);
