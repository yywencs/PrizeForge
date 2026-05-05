package award

import "github.com/go-kratos/kratos/v2/errors"

var (
	ErrorTaskPayloadMarshal = errors.InternalServer("TASK_PAYLOAD_MARSHAL_ERROR", "任务负载序列化失败")
)
