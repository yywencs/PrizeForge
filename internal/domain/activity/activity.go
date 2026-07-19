package activity

import "time"

const (
	// TaskTypeActivitySkuStockConsume 定义活动SKU库存消费任务类型
	TaskTypeActivitySkuStockConsume = "activity:sku_stock_consume"

	// TaskTypeActivityStateSync 定义活动状态同步任务类型
	TaskTypeActivityStateSync = "activity:state_sync"

	// ActivitySkuStockZeroTopic 定义活动SKU库存归零消息Topic
	ActivitySkuStockZeroTopic = "activity_sku_stock_zero_topic"

	ActivityAwardSendTopic = "activity_award_send_topic"
	SaveOrderRecordTopic   = "save_order_record"
)

const (
	QueueNameSkuStock = "queue:activity_sku_stock"
)

type ActivityCount struct {
	ActivityCountID int64
	TotalCount      int
	DayCount        int
	MonthCount      int
}

type ActivityAccount struct {
	UserID            string
	ActivityID        int64
	TotalCount        int
	TotalCountSurplus int
	DayCount          int
	DayCountSurplus   int
	MonthCount        int
	MonthCountSurplus int
	// CurrentOrderID 已扣额度但尚未完成的抽奖订单。空值表示没有进行中的抽奖。
	CurrentOrderID string
}

// ActivityAccountMonth 对应月维度活动账户实体
type ActivityAccountMonth struct {
	// 用户ID
	UserID string
	// 活动ID
	ActivityID int64
	// 月（yyyy-mm）
	Month string
	// 月次数
	MonthCount int
	// 月次数-剩余
	MonthCountSurplus int
}

// ActivityAccountDay 对应日维度活动账户实体
type ActivityAccountDay struct {
	// 用户ID
	UserID string
	// 活动ID
	ActivityID int64
	// 日期（yyyy-mm-dd）
	Day string
	// 日次数
	DayCount int
	// 日次数-剩余
	DayCountSurplus int
}

type ActivityOrder struct {
	UserID        string
	Sku           int64
	ActivityID    int64
	ActivityName  string
	StrategyID    int64
	OrderID       string
	OrderTime     time.Time
	TotalCount    int
	DayCount      int
	MonthCount    int
	State         string
	OutBusinessNo string
}

type PartakeRaffleActivity struct {
	UserID     string
	ActivityID int64
	// RequestID 由客户端为一次点击生成；同一次点击的所有重试必须复用该值。
	RequestID string
}

type UserRaffleOrder struct {
	// 用户ID
	UserID string
	// 活动ID
	ActivityID int64
	// 活动名称
	ActivityName string
	// 抽奖策略ID
	StrategyID int64
	// 订单ID
	OrderID string
	// 客户端请求幂等ID
	RequestID string
	// 下单时间
	OrderTime time.Time
	// 订单状态；create-创建、used-已使用、cancel-已作废
	OrderState UserRaffleOrderState
	// 抽奖执行状态；created-待执行、processing-执行中、success-已完成、cancelled-已取消
	DrawState DrawState
	// 抢占执行权的时间，用于执行实例宕机后的超时接管
	ProcessingAt *time.Time
	// DrawOwner 当前执行者令牌；超时接管后用于阻止旧执行者提交结果。
	DrawOwner string
}

type ActivitySku struct {
	Sku               int64
	ActivityID        int64
	ActivityCountID   int64
	StockCount        int
	StockCountSurplus int
}

type Activity struct {
	ActivityID    int64
	ActivityName  string
	ActivityDesc  string
	BeginDateTime time.Time
	EndDateTime   time.Time
	StrategyID    int64
	State         ActivityState
}

type CreateQuotaOrder struct {
	// UserID 用户ID
	UserID string
	// ActivityID 活动ID
	ActivityID int64
	// TotalCount 增加；总次数
	TotalCount int
	// DayCount 增加；日次数
	DayCount int
	// MonthCount 增加；月次数
	MonthCount int
	// ActivityOrder 活动订单实体
	ActivityOrder *ActivityOrder
}

// CreatePartakeOrderAggregate 创建参与订单聚合对象
type CreatePartakeOrder struct {
	// UserID 用户ID
	UserID string
	// ActivityID 活动ID
	ActivityID int64
	// ActivityAccount 账户总额度
	ActivityAccount *ActivityAccount
	// IsExistAccountMonth 是否存在月账户
	IsExistAccountMonth bool
	// ActivityAccountMonth 账户月额度
	ActivityAccountMonth *ActivityAccountMonth
	// IsExistAccountDay 是否存在日账户
	IsExistAccountDay bool
	// ActivityAccountDay 账户日额度
	ActivityAccountDay *ActivityAccountDay
	// UserRaffleOrder 抽奖单实体
	UserRaffleOrder *UserRaffleOrder
	// Reused 是否复用了未消费的 pending 订单（true 表示这是重试/复用路径）
	Reused bool
}

type SaveOrderTaskMessage struct {
	UserID  string `json:"u"`
	OrderID string `json:"o"`
}

func (m *SaveOrderTaskMessage) ToCreatePartakeOrder() *CreatePartakeOrder {
	return &CreatePartakeOrder{
		UserID:          m.UserID,
		UserRaffleOrder: &UserRaffleOrder{UserID: m.UserID, OrderID: m.OrderID},
	}
}

type SkuRecharge struct {
	UserID        string
	Sku           int64
	OutBusinessNo string
}

type ActivitySkuStockKey struct {
	/** 商品sku */
	Sku int64
	/** 活动ID */
	ActivityID int64
}

type ActivityState string

const (
	ActivityStateCreate ActivityState = "create"
	ActivityStateOpen   ActivityState = "open"
	ActivityStateClose  ActivityState = "close"
)

func (s ActivityState) Desc() string {
	switch s {
	case ActivityStateCreate:
		return "创建"
	case ActivityStateOpen:
		return "开启"
	case ActivityStateClose:
		return "关闭"
	default:
		return "未知"
	}
}

type UserRaffleOrderState string

const (
	UserRaffleOrderStateCreate UserRaffleOrderState = "create"
	UserRaffleOrderStateUsed   UserRaffleOrderState = "used"
	UserRaffleOrderStateCancel UserRaffleOrderState = "cancel"
)

func (s UserRaffleOrderState) Desc() string {
	switch s {
	case UserRaffleOrderStateCreate:
		return "创建"
	case UserRaffleOrderStateUsed:
		return "已使用"
	case UserRaffleOrderStateCancel:
		return "已作废"
	default:
		return "未知"
	}
}

type DrawState string

const (
	DrawStateCreated    DrawState = "created"
	DrawStateProcessing DrawState = "processing"
	DrawStateSuccess    DrawState = "success"
	DrawStateCancelled  DrawState = "cancelled"
)

type DrawClaimStatus string

const (
	DrawClaimAcquired   DrawClaimStatus = "acquired"
	DrawClaimProcessing DrawClaimStatus = "processing"
	DrawClaimCompleted  DrawClaimStatus = "completed"
	DrawClaimCancelled  DrawClaimStatus = "cancelled"
)

type DrawClaim struct {
	Status DrawClaimStatus
	Owner  string
}

type AccountSyncState string

const (
	AccountSyncStateCreate    AccountSyncState = "create"
	AccountSyncStateCompleted AccountSyncState = "completed"
	AccountSyncStateFail      AccountSyncState = "fail"
)

const (
	ActivityOrderStateCompleted = "completed"
)

// ActivityResult 抽奖结果存储结构
type ActivityResult struct {
	// 用户ID
	UserID string `json:"u"`
	// 状态：1-成功，2-兜底积分
	Status int `json:"s"`
	// 结果：奖品ID或积分值
	Result string `json:"r"`
	// 时间戳：中奖时间戳
	Timestamp int64 `json:"t"`
}

// ActivityResultStatus 抽奖结果状态常量
const (
	ActivityResultStatusSuccess = 1 // 抽奖成功
	ActivityResultStatusCredit  = 2 // 兜底积分
)

// ActivityResultPointsPrefix 积分结果标识前缀
const (
	ActivityResultPointsPrefix = "POINTS" // 积分标识前缀
)
