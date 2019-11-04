package server

import (
	"context"

	"github.com/labstack/echo"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sapcc/maria-back-me-up/pkg/constants"
	"github.com/sapcc/maria-back-me-up/pkg/log"
)

type Server struct {
	echo *echo.Echo
}

func NewServer(e *echo.Echo) *Server {
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	return &Server{
		echo: e,
	}
}

func (s *Server) Start() (err error) {
	if err = s.echo.Start(":" + constants.PORT); err != nil {
		log.Error("shutting down the server")
		return
	}
	return
}

func (s *Server) Stop(ctx context.Context) {
	if err := s.echo.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}
