package server

import (
	"prizeforge/internal/application/admin"
	"prizeforge/internal/application/api"
	httpserver "prizeforge/server/http"
	adminhttp "prizeforge/server/http/admin"
	apihttp "prizeforge/server/http/api"
)

// NewAPIServer creates the API HTTP server for frontend raffle traffic.
func NewAPIServer(addr string, strategyUsecase *api.StrategyUsecase, activityUsecase *api.ActivityUsecase) httpserver.Server {
	return apihttp.NewServer(addr, strategyUsecase, activityUsecase)
}

// NewAdminServer creates the Admin HTTP server for strategy/config management.
func NewAdminServer(addr string, strategyUsecase *admin.StrategyUsecase) httpserver.Server {
	return adminhttp.NewServer(addr, strategyUsecase)
}
