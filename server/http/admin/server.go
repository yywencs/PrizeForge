package admin

import (
	"context"
	"fmt"
	"net/http"

	"prizeforge/internal/application/admin"
	"prizeforge/internal/middleware"

	"github.com/gin-gonic/gin"
)

type Server struct {
	engine          *gin.Engine
	httpServer      *http.Server
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
	s.engine.Use(middleware.CORS())
	s.engine.Use(middleware.PrometheusMetrics())
	s.registerRoutes()
	return s
}

func (s *Server) Run() error {
	s.httpServer = &http.Server{
		Addr:    s.addr,
		Handler: s.engine,
	}
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("listen and serve: %w", err)
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) Engine() *gin.Engine {
	return s.engine
}
