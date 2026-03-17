package data

import "fmt"

// ============================strategy keys ==============================

const (
	strategyPrefix              = "big_market_strategy_%d"
	strategyAwardKeyPrefix      = "big_market_strategy_award_key_%d"
	strategyRateTableKeyPrefix  = "big_market_strategy_rate_table_key_%s"
	strategyRateRangeKeyPrefix  = "big_market_strategy_rate_range_key_%s"
	strategyAwardCountKey       = "big_market_strategy_award_count_key_%d_%d"
	strategyRuleModelKeyPrefix  = "big_market_strategy_rule_model_%d_%d"
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

func GetStrategyRuleModelKey(strategyID int64, awardID int64) string {
	return fmt.Sprintf(strategyRuleModelKeyPrefix, strategyID, awardID)
}

func GetRuleTreeKey(treeID string) string {
	return fmt.Sprintf(ruleTreeKeyPrefix, treeID)
}

func GetStrategyRuleWeightKey(strategyID int64) string {
	return fmt.Sprintf(strategyRuleWeightKeyPrefix, strategyID)
}

// ============================activity keys ==============================

const (
	ActivitySkuCountQueryKey = "activity_sku_count_query_key"
	activitySkuKeyPrefix     = "big_market_activity_sku_%d"
	activityKeyPrefix        = "big_market_activity_%d"
	activitySkuStockCountKey = "activity_sku_stock_count_key_%d"
	activityCountKeyPrefix   = "big_market_activity_count_%d"
	activityAccountKey       = "activity_account_key_%d_%s"
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

func GetActivitySkuStockCountKey(skuID int64) string {
	return fmt.Sprintf(activitySkuStockCountKey, skuID)
}
