package server

import (
	"context"
	"net/http"

	"github.com/labstack/echo"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	if err = s.echo.Start(":8081"); err != nil {
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

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if true {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
}
