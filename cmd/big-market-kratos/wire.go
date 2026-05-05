//go:build wireinject
// +build wireinject

// The build tag makes sure the stub is not built in the final build.

package main

import (
	"big-market-kratos/internal/biz"
	"big-market-kratos/internal/conf"
	"big-market-kratos/internal/data"
	"big-market-kratos/internal/server"
	"big-market-kratos/internal/service"

	"big-market-kratos/internal/dcc"
	"big-market-kratos/internal/job"
	"big-market-kratos/internal/listener"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

// wireApp init kratos application.
func wireApp(*conf.Server, *conf.Data, *conf.RabbitMQ, *conf.Asynq, log.Logger, dcc.ConfigGetter) (*kratos.App, func(), error) {
	panic(wire.Build(server.ProviderSet, data.ProviderSet, biz.ProviderSet, service.ProviderSet, job.ProviderSet, listener.ProviderSet, newApp))
}
