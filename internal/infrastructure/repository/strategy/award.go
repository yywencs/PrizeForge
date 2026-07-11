package strategy

import (
	strategybiz "prizeforge/internal/domain/strategy"
	taskbiz "prizeforge/internal/domain/task"
	"prizeforge/internal/infrastructure/adapter"
	"prizeforge/internal/infrastructure/repository/po"
	"prizeforge/pkg/cache"
	"prizeforge/pkg/common"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"gorm.io/gorm"
)

// 获取strategyID的全部StrategyAward
func (sr *strategyRepository) QueryStrategyAwardList(ctx context.Context, strategyID int64) ([]*strategybiz.StrategyAward, error) {
	var strategyAwards []*strategybiz.StrategyAward

	cacheKey := adapter.GetStrategyAwardKey(strategyID)

	err := sr.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &strategyAwards,
		TTL:   10 * 24 * time.Hour,

		Do: func(*cache.Item) (interface{}, error) {
			var dbResult []po.StrategyAward
			err := sr.db.WithContext(ctx).
				Where("strategy_id = ?", strategyID).
				Find(&dbResult).Error

			if err != nil {
				return nil, err
			}

			var entities []*strategybiz.StrategyAward
			for _, po := range dbResult {
				entities = append(entities, po.ToEntity())
			}

			return entities, nil
		},
	})

	if err != nil {
		return nil, err
	}

	return strategyAwards, nil
}

func (d *strategyRepository) QueryStrategyAward(ctx context.Context, strategyID int64, awardID int64) (*strategybiz.StrategyAward, error) {
	var strategyAward strategybiz.StrategyAward

	cacheKey := fmt.Sprintf("%s:%d", adapter.GetStrategyAwardKey(strategyID), awardID)

	err := d.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &strategyAward,
		TTL:   10 * 24 * time.Hour,

		Do: func(*cache.Item) (interface{}, error) {
			var dbResult po.StrategyAward
			err := d.db.WithContext(ctx).
				Where("strategy_id = ? AND award_id = ?", strategyID, awardID).
				First(&dbResult).Error

			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, nil
				}
				return nil, err
			}

			return dbResult.ToEntity(), nil
		},
	})

	if err != nil {
		return nil, err
	}

	return &strategyAward, nil
}

// UpdateStrategyAwardStock 根据队列消费结果，持久化扣减库存到数据库
func (d *strategyRepository) UpdateStrategyAwardStock(ctx context.Context, strategyID int64, awardID int64) error {
	tx := d.db.WithContext(ctx).
		Model(&po.StrategyAward{}).
		Where("strategy_id = ? AND award_id = ? AND award_count_surplus > 0", strategyID, awardID).
		Update("award_count_surplus", gorm.Expr("award_count_surplus - 1"))

	if tx.Error != nil {
		return tx.Error
	}
	if tx.RowsAffected == 0 {
		return errors.New("no stock to consume or record not found")
	}
	return nil
}

// NewAwardStockConsumeHandler 返回一个可直接用于 ConsumeAwardStockQueue 的处理函数
func NewAwardStockConsumeHandler(ctx context.Context, repo *strategyRepository) func(msgs []taskbiz.AwardStockConsumeMessage) error {
	return func(msgs []taskbiz.AwardStockConsumeMessage) error {
		for _, m := range msgs {
			if err := repo.UpdateStrategyAwardStock(ctx, m.StrategyID, m.AwardID); err != nil {
				return err
			}
		}
		return nil
	}
}

// QueryAwardRuleWeight 查询策略规则权重配置
func (d *strategyRepository) QueryAwardRuleWeight(ctx context.Context, strategyID int64) ([]*strategybiz.WeightBucket, error) {
	var ruleWeightVOList []*strategybiz.WeightBucket

	cacheKey := adapter.GetStrategyRuleWeightKey(strategyID)
	err := d.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &ruleWeightVOList,
		TTL:   10 * 24 * time.Hour,
		Do: func(*cache.Item) (interface{}, error) {
			var rulePO po.StrategyRule
			err := d.db.WithContext(ctx).
				Where("strategy_id = ? AND rule_model = ?", strategyID, "rule_weight").
				First(&rulePO).Error

			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, nil
				}
				return nil, err
			}

			ruleValueGroups := strings.Split(rulePO.RuleValue, common.SPACE)

			uniqueAwardIDs := make(map[int]struct{})

			type parsedRule struct {
				weightStr string
				weight    int
				awardIDs  []int
			}
			var parsedRules []parsedRule

			for _, group := range ruleValueGroups {
				if group == "" {
					continue
				}
				parts := strings.Split(group, common.COLON)
				if len(parts) != 2 {
					continue
				}

				weightStr := parts[0]
				awardIDsStr := parts[1]

				weight, err := strconv.Atoi(weightStr)
				if err != nil {
					return nil, err
				}

				var awardIDs []int
				idStrs := strings.Split(awardIDsStr, common.SPLIT)
				for _, idStr := range idStrs {
					id, err := strconv.Atoi(idStr)
					if err != nil {
						continue
					}
					awardIDs = append(awardIDs, id)
					uniqueAwardIDs[id] = struct{}{}
				}

				parsedRules = append(parsedRules, parsedRule{
					weightStr: group,
					weight:    weight,
					awardIDs:  awardIDs,
				})
			}

			awardIDList := make([]int64, 0, len(uniqueAwardIDs))
			for id := range uniqueAwardIDs {
				awardIDList = append(awardIDList, int64(id))
			}

			awardTitleMap := make(map[int]string)
			if len(awardIDList) > 0 {
				var awardPOs []po.StrategyAward
				err = d.db.WithContext(ctx).
					Where("strategy_id = ? AND award_id IN ?", strategyID, awardIDList).
					Find(&awardPOs).Error
				if err != nil {
					return nil, err
				}
				for _, po := range awardPOs {
					awardTitleMap[int(po.AwardID)] = po.AwardTitle
				}
			}

			var voList []*strategybiz.WeightBucket
			for _, rule := range parsedRules {
				var awardList []strategybiz.Award
				for _, id := range rule.awardIDs {
					title := awardTitleMap[id]
					awardList = append(awardList, strategybiz.Award{
						AwardId:    id,
						AwardTitle: title,
					})
				}

				voList = append(voList, &strategybiz.WeightBucket{
					RuleValue: rule.weightStr,
					Weight:    rule.weight,
					AwardIds:  rule.awardIDs,
					AwardList: awardList,
				})
			}

			return voList, nil
		},
	})

	if err != nil {
		return nil, err
	}

	return ruleWeightVOList, nil
}

func (d *strategyRepository) SubtractionAwardStock(ctx context.Context, strategyID int64, awardID int64) (bool, error) {
	cacheKey := adapter.GetStrategyAwardCountKey(strategyID, awardID)

	surplus, err := d.redis.Decr(ctx, cacheKey)
	if err != nil {
		return false, err
	}

	if surplus < 0 {
		return false, nil
	}

	lockKey := fmt.Sprintf("%s_%d", cacheKey, surplus)
	ok, err := d.redis.SetNX(ctx, lockKey, "locked", time.Hour*24)
	if err != nil {
		return false, err
	}

	return ok, nil
}

func (d *strategyRepository) AwardStockConsumeSendQueue(ctx context.Context, strategyID int64, awardID int64) error {
	msg := taskbiz.AwardStockConsumeMessage{
		StrategyID: strategyID,
		AwardID:    awardID,
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	t := asynq.NewTask(taskbiz.TaskTypeStrategyAwardStockConsume, payload)
	_, err = d.queue.Enqueue(t, asynq.Queue("critical"), asynq.ProcessIn(1*time.Second))
	return err
}
