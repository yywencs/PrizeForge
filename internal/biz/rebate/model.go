package rebate

type BehaviorType string

const (
	Sign      BehaviorType = "sign"
	OpenAiPay BehaviorType = "openai_pay"
)

type RebateType string

const (
	Sku      RebateType = "sku"
	Integral RebateType = "integral"
)

type Behavior struct {
	UserID        string
	BehaviorType  BehaviorType // valobj.BehaviorType
	OutBusinessNo string       // 业务ID
}

type BehaviorRebateOrder struct {
	UserID        string
	OrderID       string
	BehaviorType  string
	RebateDesc    string
	RebateType    string
	OutBusinessNo string
	RebateConfig  string
	BizID         string // 业务ID
}

type BehaviorRebate struct {
	UserID               string
	Behavior             *Behavior
	BehaviorRebateOrders []*BehaviorRebateOrder
}

type DailyBehaviorRebate struct {
	BehaviorType string
	RebateDesc   string
	RebateType   string
	RebateConfig string
}

type RebateMessage struct {
	UserID       string `json:"user_id"`
	RebateDesc   string `json:"rebate_desc"`
	RebateType   string `json:"rebate_type"`
	RebateConfig string `json:"rebate_config"`
	BizID        string `json:"biz_id"`
}
