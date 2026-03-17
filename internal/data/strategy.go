package data

import (
	"big-market-kratos/internal/biz/strategy"
	"big-market-kratos/internal/biz/task"
	"big-market-kratos/internal/data/po"
	"big-market-kratos/pkg/cache"
	"big-market-kratos/pkg/common"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"gorm.io/gorm"
)

// 专门负责数据库的实现
// 实现 StrategyRepository 接口方法
type strategyRepository struct {
	db       *gorm.DB
	redis    *cache.Cache
	routerDB *DBRouter
	queue    *asynq.Client
}

// 4. 构造函数分别返回
func NewStrategyRepository(db *gorm.DB, redis *cache.Cache, queue *asynq.Client, routerDB *DBRouter) strategy.Repo {
	// 实例化数据库仓储
	repo := &strategyRepository{
		db:       db,
		routerDB: routerDB,
		redis:    redis,
		queue:    queue,
	}

	return repo
}

func (sr *strategyRepository) QueryStrategyIdByActivityId(ctx context.Context, activityID int64) (int64, error) {
	var raffleActivity po.RaffleActivity
	err := sr.db.WithContext(ctx).
		Where("activity_id = ?", activityID).
		First(&raffleActivity).Error
	if err != nil {
		return 0, err
	}
	return raffleActivity.StrategyID, nil
}

// 通过StrategyId查询StrategyEntity
func (sr *strategyRepository) QueryStrategyEntityByStrategyId(ctx context.Context, strategyID int64) (*strategy.Strategy, error) {
	var strategyEntity strategy.Strategy

	cacheKey := GetStrategyKey(strategyID)

	err := sr.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &strategyEntity,
		TTL:   10 * time.Minute,

		Do: func(*cache.Item) (interface{}, error) {
			var dbResult po.Strategy
			// 查库
			err := sr.db.WithContext(ctx).
				Where("strategy_id = ?", strategyID).
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

	return &strategyEntity, nil
}

// 获取strategyID的全部StrategyAward
func (sr *strategyRepository) QueryStrategyAwardList(ctx context.Context, strategyID int64) ([]*strategy.StrategyAward, error) {
	var strategyAwards []*strategy.StrategyAward

	cacheKey := GetStrategyAwardKey(strategyID)

	err := sr.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &strategyAwards, // 结果会填入这里
		TTL:   10 * time.Minute,

		// 只有缓存未命中，才会执行这个 Do 函数
		Do: func(*cache.Item) (interface{}, error) {
			var dbResult []po.StrategyAward
			err := sr.db.WithContext(ctx).
				Where("strategy_id = ?", strategyID).
				Find(&dbResult).Error

			if err != nil {
				return nil, err
			}

			var entities []*strategy.StrategyAward
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

// QueryStrategyRule 根据 strategyID 和 ruleModel 查询策略规则
func (sr *strategyRepository) QueryStrategyRule(ctx context.Context, strategyID int64, ruleModel string) (*strategy.StrategyRule, error) {
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

// QueryStrategyValue 根据 strategyID、awardID 和 ruleModel 查询策略规则值（通常是抽奖中规则）
func (sr *strategyRepository) QueryStrategyValue(ctx context.Context, strategyID int64, ruleModel strategy.RuleChainName) (string, error) {
	var ruleValue string

	err := sr.db.WithContext(ctx).
		Where("strategy_id = ? AND rule_model = ?", strategyID, ruleModel).
		Table("strategy_rule").
		Select("rule_value").
		Scan(&ruleValue).
		Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", nil
		}
		return "", err
	}
	return ruleValue, nil
}

// QueryStrategyRuleValue 根据 strategyID 和 ruleModel 查询策略规则值（通常是抽奖前规则）
func (sr *strategyRepository) QueryStrategyRuleValue(ctx context.Context, strategyID int64, ruleModel strategy.RuleChainName) (string, error) {
	var ruleValue string

	err := sr.db.WithContext(ctx).
		Where("strategy_id = ? AND rule_model = ?", strategyID, ruleModel).
		Table("strategy_rule").
		Select("rule_value").
		Scan(&ruleValue).
		Error

	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", nil
		}
		return "", err
	}
	return ruleValue, nil
}

func (sr *strategyRepository) QueryStrategyRuleModel(ctx context.Context, strategyID int64, awardID int64) (strategy.RuleTreeName, error) {
	var ruleModel strategy.RuleTreeName

	cacheKey := GetStrategyRuleModelKey(strategyID, awardID)

	err := sr.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &ruleModel,
		TTL:   5 * time.Minute,

		Do: func(*cache.Item) (interface{}, error) {
			var dbResult po.StrategyAward
			// 查库
			err := sr.db.WithContext(ctx).
				Where("strategy_id = ? AND award_id = ?", strategyID, awardID).
				First(&dbResult).Error

			if errors.Is(err, gorm.ErrRecordNotFound) {
				// 没有配置：返回空字符串 + nil，顺带可以被缓存下来
				return strategy.RuleTreeName(""), nil
			}

			if err != nil {
				return nil, err
			}

			return dbResult.RuleModels, nil
		},
	})

	if err != nil {
		return strategy.RuleTreeName(""), err
	}
	return ruleModel, nil
}

func (d *strategyRepository) QueryRuleTree(ctx context.Context, ruleModel strategy.RuleTreeName) (*strategy.RuleTree, error) {
	var ruleTree strategy.RuleTree

	cacheKey := GetRuleTreeKey(string(ruleModel))

	err := d.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &ruleTree,
		TTL:   10 * time.Minute,

		Do: func(*cache.Item) (interface{}, error) {
			var treePO po.RuleTree
			if err := d.db.WithContext(ctx).
				Where("tree_id = ?", string(ruleModel)).
				First(&treePO).Error; err != nil {
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

			nodeLineMap := make(map[string][]*strategy.RuleTreeNodeLine, len(linePOs))
			for _, po := range linePOs {
				line := po.ToEntity()
				nodeLineMap[po.RuleNodeFrom] = append(nodeLineMap[po.RuleNodeFrom], line)
			}

			nodeMap := make(map[strategy.RuleTreeName]*strategy.RuleTreeNode, len(nodePOs))
			for _, po := range nodePOs {
				node := po.ToEntity()
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

// UpdateStrategyAwardStock 根据队列消费结果，持久化扣减库存到数据库
// 将 strategy_award.award_count_surplus -= 1，前提 award_count_surplus > 0
func (d *strategyRepository) UpdateStrategyAwardStock(ctx context.Context, strategyID int64, awardID int64) error {
	tx := d.db.WithContext(ctx).
		Model(&po.StrategyAward{}).
		Where("strategy_id = ? AND award_id = ? AND award_count_surplus > 0", strategyID, awardID).
		Update("award_count_surplus", gorm.Expr("award_count_surplus - 1"))

	if tx.Error != nil {
		return tx.Error
	}
	// 若受影响行数为 0，说明没有可用库存或记录不存在
	if tx.RowsAffected == 0 {
		return errors.New("no stock to consume or record not found")
	}
	return nil
}

// NewAwardStockConsumeHandler 返回一个可直接用于 ConsumeAwardStockQueue 的处理函数
func NewAwardStockConsumeHandler(ctx context.Context, repo *strategyRepository) func(msgs []task.AwardStockConsumeMessage) error {
	return func(msgs []task.AwardStockConsumeMessage) error {
		for _, m := range msgs {
			if err := repo.UpdateStrategyAwardStock(ctx, m.StrategyID, m.AwardID); err != nil {
				return err
			}
		}
		return nil
	}
}

func (d *strategyRepository) QueryStrategyAward(ctx context.Context, strategyID int64, awardID int64) (*strategy.StrategyAward, error) {
	var strategyAward strategy.StrategyAward

	cacheKey := GetStrategyAwardKey(strategyID) + ":" + string(rune(awardID))

	err := d.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &strategyAward,
		TTL:   10 * time.Minute,

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

// QueryAwardRuleWeight 查询策略规则权重配置
func (d *strategyRepository) QueryAwardRuleWeight(ctx context.Context, strategyID int64) ([]*strategy.WeightBucket, error) {
	var ruleWeightVOList []*strategy.WeightBucket

	cacheKey := GetStrategyRuleWeightKey(strategyID)
	err := d.redis.Once(&cache.Item{
		Ctx:   ctx,
		Key:   cacheKey,
		Value: &ruleWeightVOList,
		TTL:   10 * time.Minute,
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

			// 2. 解析规则值
			ruleValueGroups := strings.Split(rulePO.RuleValue, common.SPACE) // 使用空格分隔

			// 收集所有需要查询的 AwardID
			uniqueAwardIDs := make(map[int]struct{})

			// 临时存储解析结果
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
				// 分割 权重:奖品ID列表
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

			// 3. 批量查询奖品标题
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

			// 4. 组装 VO
			var voList []*strategy.WeightBucket
			for _, rule := range parsedRules {
				var awardList []strategy.Award
				for _, id := range rule.awardIDs {
					title := awardTitleMap[id]
					awardList = append(awardList, strategy.Award{
						AwardId:    id,
						AwardTitle: title,
					})
				}

				voList = append(voList, &strategy.WeightBucket{
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

func (d *strategyRepository) QueryActivityAccountTotalUseCount(ctx context.Context, userID string, strategyID int64) (int64, error) {
	// 1. 根据 strategyID 查询 activityID
	var activityPO po.RaffleActivity
	err := d.db.WithContext(ctx).
		Where("strategy_id = ?", strategyID).
		First(&activityPO).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, nil
		}
		return 0, err
	}

	// 2. 查询 raffle_activity_account 表
	var accountPO struct {
		TotalCount        int
		TotalCountSurplus int
	}

	db, _ := d.routerDB.DBStrategy(userID)
	if db == nil {
		return 0, errors.New("db router failed")
	}
	err = db.WithContext(ctx).Table("raffle_activity_account").
		Select("total_count, total_count_surplus").
		Where("user_id = ? AND activity_id = ?", userID, activityPO.ActivityID).
		Scan(&accountPO).Error

	if err != nil {
		return 0, err
	}

	// 3. 计算已使用次数
	return int64(accountPO.TotalCount - accountPO.TotalCountSurplus), nil
}

func (d *strategyRepository) StoreStrategyAwardPool(ctx context.Context, strategyID string, rateRange int, idxToAwardIDMap map[int]int64) error {
	err := d.redis.Set(&cache.Item{
		Ctx:   ctx,
		Key:   GetStrategyRateRangeKey(strategyID),
		Value: rateRange,
		TTL:   10 * time.Minute,
	})

	if err != nil {
		return err
	}

	values := make(map[string]interface{}, len(idxToAwardIDMap))
	for k, v := range idxToAwardIDMap {
		values[strconv.Itoa(k)] = v
	}
	err = d.redis.HSetWithTTL(ctx, GetStrategyRateTableKey(strategyID), values, 10*time.Minute)

	return err
}

func (d *strategyRepository) GetRateRange(ctx context.Context, strategyID string) (int, error) {
	var rateRange int
	err := d.redis.Get(ctx, GetStrategyRateRangeKey(strategyID), &rateRange)

	return rateRange, err
}

func (d *strategyRepository) GetStrategyAwardAssemble(ctx context.Context, strategyID string, randomVal int) (int64, error) {
	valStr, err := d.redis.HGet(ctx, GetStrategyRateTableKey(strategyID), strconv.Itoa(randomVal))

	if err != nil {
		return -1, err
	}

	return strconv.ParseInt(valStr, 10, 64)
}

func (d *strategyRepository) SubtractionAwardStock(ctx context.Context, strategyID int64, awardID int64) (bool, error) {
	cacheKey := GetStrategyAwardCountKey(strategyID, awardID)

	surplus, err := d.redis.Decr(ctx, cacheKey)
	if err != nil {
		return false, err
	}

	if surplus < 0 {
		return false, nil
	}

	// 1. 按照cacheKey decr 后的值，如 99、98、97 和 key 组成为库存锁的key进行使用。
	// 2. 加锁为了兜底，如果后续有恢复库存，手动处理等，也不会超卖。因为所有的可用库存key，都被加锁了。
	lockKey := fmt.Sprintf("%s_%d", cacheKey, surplus)
	ok, err := d.redis.SetNX(ctx, lockKey, "locked", time.Hour*24)
	if err != nil {
		return false, err
	}

	if !ok {
		slog.Info("策略奖品库存加锁失败", "lockKey", lockKey)
	}

	return ok, nil
}

func (d *strategyRepository) AwardStockConsumeSendQueue(ctx context.Context, strategyID int64, awardID int64) error {
	msg := task.AwardStockConsumeMessage{
		StrategyID: strategyID,
		AwardID:    awardID,
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	task := asynq.NewTask(task.TaskTypeStrategyAwardStockConsume, payload)
	_, err = d.queue.Enqueue(task, asynq.Queue("critical"), asynq.ProcessIn(1*time.Second))
	return err
}
