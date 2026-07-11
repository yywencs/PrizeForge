package admin

import (
	"prizeforge/internal/application/admin"
	"prizeforge/server/http/common"

	"github.com/gin-gonic/gin"
)

type Server struct {
	engine          *gin.Engine
	addr            string
	strategyUsecase *admin.StrategyUsecase
}

func NewServer(addr string, strategyUsecase *admin.StrategyUsecase) *Server {
	s := &Server{
		engine:          gin.New(),
		addr:            addr,
		strategyUsecase: strategyUsecase,
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
