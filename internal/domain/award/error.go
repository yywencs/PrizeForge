package award

import "prizeforge/internal/shared/xerr"

var (
	ErrorTaskPayloadMarshal = xerr.New("TASK_PAYLOAD_MARSHAL_ERROR", "任务负载序列化失败")
)
