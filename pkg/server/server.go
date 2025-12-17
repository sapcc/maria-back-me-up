// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

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
func (s *Server) Start(port string) {
	if err := s.echo.Start(":" + port); err != nil {
		log.Error("shutting down the server", err)
	}
}

// Stop shutsdown the server
func (s *Server) Stop(ctx context.Context) {
	if err := s.echo.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
}
