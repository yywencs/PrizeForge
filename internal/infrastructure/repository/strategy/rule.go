package strategy

import (
	strategybiz "prizeforge/internal/domain/strategy"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/pkg/cache"
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
)

// QueryStrategyRule 根据 strategyID 和 ruleModel 查询策略规则
func (sr *strategyRepository) QueryStrategyRule(ctx context.Context, strategyID int64, ruleModel string) (*strategybiz.StrategyRule, error) {
	var dbResult po.StrategyRule

	err := sr.db.WithContext(ctx).
		Where("strategy_id = ? AND rule_model = ?", strategyID, ruleModel).
		First(&dbResult).Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}

	return dbResult.ToEntity(), nil
}

// QueryStrategyValue 根据 strategyID 和 ruleModel 查询策略规则值
func (sr *strategyRepository) QueryStrategyValue(ctx context.Context, strategyID int64, ruleModel strategybiz.RuleChainName) (string, error) {
	var ruleValue string
	cacheKey := adapter.GetStrategyRuleValueKey(strategyID, string(ruleModel))

	err := sr.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &ruleValue,
		TTL:   10 * 24 * time.Hour,

		Do: func(*cache.Item) (interface{}, error) {
			var rv string
			err := sr.db.WithContext(ctx).
				Where("strategy_id = ? AND rule_model = ?", strategyID, ruleModel).
				Table("strategy_rule").
				Select("rule_value").
				Scan(&rv).
				Error

			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return "", nil
				}
				return "", err
			}
			return rv, nil
		},
	})

	if err != nil {
		return "", err
	}
	return ruleValue, nil
}

// QueryStrategyRuleValue 根据 strategyID 和 ruleModel 查询策略规则值
func (sr *strategyRepository) QueryStrategyRuleValue(ctx context.Context, strategyID int64, ruleModel strategybiz.RuleChainName) (string, error) {
	var ruleValue string
	cacheKey := adapter.GetStrategyRuleValueKey(strategyID, string(ruleModel))

	err := sr.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &ruleValue,
		TTL:   10 * 24 * time.Hour,

		Do: func(*cache.Item) (interface{}, error) {
			var rv string
			err := sr.db.WithContext(ctx).
				Where("strategy_id = ? AND rule_model = ?", strategyID, ruleModel).
				Table("strategy_rule").
				Select("rule_value").
				Scan(&rv).
				Error

			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return "", nil
				}
				return "", err
			}
			return rv, nil
		},
	})

	if err != nil {
		return "", err
	}
	return ruleValue, nil
}

func (sr *strategyRepository) QueryStrategyRuleModel(ctx context.Context, strategyID int64, awardID int64) (strategybiz.RuleTreeName, error) {
	var ruleModel strategybiz.RuleTreeName

	cacheKey := adapter.GetStrategyRuleModelKey(strategyID, awardID)

	err := sr.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &ruleModel,
		TTL:   10 * 24 * time.Hour,

		Do: func(*cache.Item) (interface{}, error) {
			var dbResult po.StrategyAward
			err := sr.db.WithContext(ctx).
				Where("strategy_id = ? AND award_id = ?", strategyID, awardID).
				First(&dbResult).Error

			if errors.Is(err, gorm.ErrRecordNotFound) {
				return strategybiz.RuleTreeName(""), nil
			}

			if err != nil {
				return nil, err
			}

			return dbResult.RuleModels, nil
		},
	})

	if err != nil {
		return strategybiz.RuleTreeName(""), err
	}
	return ruleModel, nil
}

func (d *strategyRepository) QueryRuleTree(ctx context.Context, ruleModel strategybiz.RuleTreeName) (*strategybiz.RuleTree, error) {
	var ruleTree strategybiz.RuleTree

	cacheKey := adapter.GetRuleTreeKey(string(ruleModel))

	err := d.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &ruleTree,
		TTL:   10 * 24 * time.Hour,

		Do: func(*cache.Item) (interface{}, error) {
			var treePO po.RuleTree
			if err := d.db.WithContext(ctx).
				Where("tree_id = ?", string(ruleModel)).
				First(&treePO).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, nil
				}
				return nil, err
			}

			var nodePOs []po.RuleTreeNode
			if err := d.db.WithContext(ctx).
				Where("tree_id = ?", string(ruleModel)).
				Find(&nodePOs).Error; err != nil {
				return nil, err
			}

			var linePOs []po.RuleTreeNodeLine
			if err := d.db.WithContext(ctx).
				Where("tree_id = ?", string(ruleModel)).
				Find(&linePOs).Error; err != nil {
				return nil, err
			}

			nodeLineMap := make(map[string][]*strategybiz.RuleTreeNodeLine, len(linePOs))
			for _, po := range linePOs {
				line := po.ToEntity()
				nodeLineMap[po.RuleNodeFrom] = append(nodeLineMap[po.RuleNodeFrom], line)
			}

			nodeMap := make(map[strategybiz.RuleTreeName]*strategybiz.RuleTreeNode, len(nodePOs))
			for _, po := range nodePOs {
				node := po.ToEntity()
				if lines, ok := nodeLineMap[string(node.RuleKey)]; ok {
					node.TreeNodeLine = lines
				}
				nodeMap[node.RuleKey] = node
			}

			ruleTreeEntity := treePO.ToEntity()
			ruleTreeEntity.NodeMap = nodeMap
			return ruleTreeEntity, nil
		},
	})

	if err != nil {
		return nil, err
	}
	return &ruleTree, nil
}
