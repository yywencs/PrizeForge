package adapter

import "fmt"

// ============================strategy keys ==============================

const (
	strategyPrefix              = "big_market_strategy_%d"
	strategyAwardKeyPrefix      = "big_market_strategy_award_key_%d"
	strategyRateTableKeyPrefix  = "big_market_strategy_rate_table_key_%s"
	strategyRateRangeKeyPrefix  = "big_market_strategy_rate_range_key_%s"
	strategyAwardCountKey       = "big_market_strategy_award_count_key_%d_%d"
	strategyAwardReservationKey = "big_market_strategy_award_reservation_%s_%s"
	strategyRuleModelKeyPrefix  = "big_market_strategy_rule_model_%d_%d"
	strategyRuleValueKeyPrefix  = "big_market_strategy_rule_value_key_%d_%s"
	ruleTreeKeyPrefix           = "big_market_rule_tree_%s"
	strategyRuleWeightKeyPrefix = "big_market_strategy_rule_weight_key_%d"
)

func GetStrategyKey(strategyID int64) string {
	return fmt.Sprintf(strategyPrefix, strategyID)
}

func GetStrategyAwardKey(strategyID int64) string {
	return fmt.Sprintf(strategyAwardKeyPrefix, strategyID)
}

func GetStrategyRateTableKey(strategyID string) string {
	return fmt.Sprintf(strategyRateTableKeyPrefix, strategyID)
}

func GetStrategyRateRangeKey(strategyID string) string {
	return fmt.Sprintf(strategyRateRangeKeyPrefix, strategyID)
}

func GetStrategyAwardCountKey(strategyID int64, awardID int64) string {
	return fmt.Sprintf(strategyAwardCountKey, strategyID, awardID)
}

func GetStrategyAwardReservationKey(userID string, orderID string) string {
	return fmt.Sprintf(strategyAwardReservationKey, userID, orderID)
}

func GetStrategyRuleModelKey(strategyID int64, awardID int64) string {
	return fmt.Sprintf(strategyRuleModelKeyPrefix, strategyID, awardID)
}

func GetStrategyRuleValueKey(strategyID int64, ruleModel string) string {
	return fmt.Sprintf(strategyRuleValueKeyPrefix, strategyID, ruleModel)
}

func GetRuleTreeKey(treeID string) string {
	return fmt.Sprintf(ruleTreeKeyPrefix, treeID)
}

func GetStrategyRuleWeightKey(strategyID int64) string {
	return fmt.Sprintf(strategyRuleWeightKeyPrefix, strategyID)
}

// ============================activity keys ==============================

const (
	ActivitySkuCountQueryKey       = "activity_sku_count_query_key"
	activitySkuKeyPrefix           = "big_market_activity_sku_%d"
	activityKeyPrefix              = "big_market_activity_%d"
	activitySkuStockCountKey       = "activity_sku_stock_count_key_%d"
	activityCountKeyPrefix         = "big_market_activity_count_%d"
	activityAccountKey             = "activity_account_key_%d_%s"
	activityAccountTotalSurplusKey = "activity_account_total_surplus_%d_%s"
	activityAccountMonthSurplusKey = "activity_account_month_surplus_%d_%s_%s"
	activityAccountDaySurplusKey   = "activity_account_day_surplus_%d_%s_%s"
	pendingRaffleOrderKey          = "pending_raffle_order_%d_%s"
	activityResultHashKey          = "activity_result_hash_%d_%d" // activityID + 分片号
)

func GetActivitySkuKey(sku int64) string {
	return fmt.Sprintf(activitySkuKeyPrefix, sku)
}

func GetActivityKey(activityID int64) string {
	return fmt.Sprintf(activityKeyPrefix, activityID)
}

func GetActivityCountKey(activityCountID int64) string {
	return fmt.Sprintf(activityCountKeyPrefix, activityCountID)
}

func GetActivityAccountKey(activityID int64, userID string) string {
	return fmt.Sprintf(activityAccountKey, activityID, userID)
}

func GetActivityAccountTotalSurplusKey(activityID int64, userID string) string {
	return fmt.Sprintf(activityAccountTotalSurplusKey, activityID, userID)
}

func GetActivityAccountMonthSurplusKey(activityID int64, userID string, month string) string {
	return fmt.Sprintf(activityAccountMonthSurplusKey, activityID, userID, month)
}

func GetActivityAccountDaySurplusKey(activityID int64, userID string, day string) string {
	return fmt.Sprintf(activityAccountDaySurplusKey, activityID, userID, day)
}

func GetPendingRaffleOrderKey(activityID int64, userID string) string {
	return fmt.Sprintf(pendingRaffleOrderKey, activityID, userID)
}

func GetActivityResultHashKey(activityID int64, shard int) string {
	return fmt.Sprintf(activityResultHashKey, activityID, shard)
}

func GetActivitySkuStockCountKey(skuID int64) string {
	return fmt.Sprintf(activitySkuStockCountKey, skuID)
}
