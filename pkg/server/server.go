package server

import (
	"context"

	"github.com/labstack/echo"
	"github.com/sapcc/maria-back-me-up/pkg/log"
)

// Server struct is ...
type Server struct {
	echo *echo.Echo
}

// NewServer creates a server instance
func NewServer(e *echo.Echo) *Server {
	return &Server{
		echo: e,
	}
}

// Start starts the server
func (s *Server) Start(port string) (err error) {
	if err = s.echo.Start(":" + port); err != nil {
		log.Error("shutting down the server", err)
		return
	}
	return
}

// Stop shutsdown the server
func (s *Server) Stop(ctx context.Context) {
	if err := s.echo.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}
