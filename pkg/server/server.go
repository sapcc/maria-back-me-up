package server

import (
	"context"

	"github.com/labstack/echo"
	"github.com/sapcc/maria-back-me-up/pkg/log"
)

type Server struct {
	echo *echo.Echo
}

func NewServer(e *echo.Echo) *Server {
	return &Server{
		echo: e,
	}
}

func (s *Server) Start(port string) (err error) {
	if err = s.echo.Start(":" + port); err != nil {
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
