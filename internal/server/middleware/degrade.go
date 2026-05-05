package middleware

import (
	"big-market-kratos/internal/dcc"
	"context"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/middleware"
)

func DegradeMiddleware(dcc dcc.ConfigGetter) middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			if dcc == nil {
				return handler(ctx, req)
			}
			// 从 DCC 获取降级配置
			enableDegrade := dcc.Get().GetEnableDegrade()
			if enableDegrade {
				return nil, errors.ServiceUnavailable("DEGRADED", "该服务已降级，请稍后再试")
			}

			return handler(ctx, req)
		}
	}
}
