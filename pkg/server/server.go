/**
 * Copyright 2024 SAP SE
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
