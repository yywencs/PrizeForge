package api

import (
	"prizeforge/internal/application/api"

	"github.com/gin-gonic/gin"
	"prizeforge/server/http/common"
)

type Server struct {
	engine          *gin.Engine
	addr            string
	strategyUsecase *api.StrategyUsecase
	activityUsecase *api.ActivityUsecase
}

func NewServer(addr string, strategyUsecase *api.StrategyUsecase, activityUsecase *api.ActivityUsecase) *Server {
	s := &Server{
		engine:          gin.New(),
		addr:            addr,
		strategyUsecase: strategyUsecase,
		activityUsecase: activityUsecase,
	}

	s.engine.Use(gin.Recovery())
	s.engine.Use(common.CrosHandler())
	s.registerRoutes()
	return s
}

func (s *Server) Run() error {
	return s.engine.Run(s.addr)
}

func (s *Server) Engine() *gin.Engine {
	return s.engine
}
