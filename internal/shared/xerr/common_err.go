package xerr

const (
	Success          ErrCode = "0000"
	Unknown          ErrCode = "0001"
	InvalidParams    ErrCode = "0002"
	DBIndexDuplicate ErrCode = "0003"
	DBRouterError    ErrCode = "ERR_BIZ_003"
)

var commonMsg = map[ErrCode]string{
	Success:          "调用成功",
	Unknown:          "调用失败",
	InvalidParams:    "非法参数",
	DBIndexDuplicate: "唯一索引冲突",
	DBRouterError:    "数据库路由失败",
}
