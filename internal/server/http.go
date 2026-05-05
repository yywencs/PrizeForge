package server

import (
	v1 "big-market-kratos/api/bigmarket/v1"
	"big-market-kratos/internal/conf"
	"big-market-kratos/internal/dcc"
	"big-market-kratos/internal/service"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/transport/http"
)

// NewHTTPServer new an HTTP server.
func NewHTTPServer(c *conf.Server, strategy *service.StrategyService, activity *service.ActivityService, logger log.Logger, dcc dcc.ConfigGetter) *http.Server {
	var opts = []http.ServerOption{
		http.Middleware(
			recovery.Recovery(),
			// middleware.DegradeMiddleware(dcc),
		),
	}
	if c.Http.Network != "" {
		opts = append(opts, http.Network(c.Http.Network))
	}
	if c.Http.Addr != "" {
		opts = append(opts, http.Address(c.Http.Addr))
	}
	if c.Http.Timeout != nil {
		opts = append(opts, http.Timeout(c.Http.Timeout.AsDuration()))
	}
	srv := http.NewServer(opts...)
	v1.RegisterStrategyHTTPServer(srv, strategy)
	v1.RegisterActivityHTTPServer(srv, activity)
	return srv
}
