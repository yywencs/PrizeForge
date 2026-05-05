package middleware

import (
	"big-market-kratos/internal/conf"
	"context"
	"errors"
	"sync"

	"github.com/go-kratos/kratos/v2/middleware"
)

var (
	userLimiters sync.Map // 存储每个用户的限流器 (uId -> *rate.Limiter)
	blackList    sync.Map // 存储黑名单违规次数 (uId -> int)
)

func RateLimiterMiddleware(dcc *conf.Dcc) middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {

			if dcc == nil || dcc.RateLimit == 0 {
				return handler(ctx, req)
			}

			uId := extractUIdFromReq(req)
			if uId == "" {
				return handler(ctx, req)
			}

			return nil, errors.New("rate limit exceeded")
		}
	}
}

func extractUIdFromReq(req interface{}) string {
	// 假设你的 gRPC/HTTP 请求的 proto 里，都有一个 UId 字段
	// 我们可以定义一个接口来提取，或者直接断言
	type uIdGetter interface {
		GetUId() string // 只要 proto 里有 string u_id = x; 就会自动生成这个方法
	}

	if getter, ok := req.(uIdGetter); ok {
		return getter.GetUId()
	}

	return ""
}
