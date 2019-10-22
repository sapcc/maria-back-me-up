/**
 * Copyright 2019 SAP SE
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

package route

import (
	"github.com/labstack/echo"
	"github.com/sapcc/maria-back-me-up/pkg/api"
	"github.com/sapcc/maria-back-me-up/pkg/backup"
)

func Init(m *backup.Manager) *echo.Echo {
	e := echo.New()
	g := e.Group("/backup")
	g.GET("/stop", api.GetGackup(m))
	g.GET("/start", api.GetGackup(m))

	gr := e.Group("/restore")
	gr.GET("/soft", api.GetRestore(m))
	gr.GET("/hard", api.GetRestore(m))
	return e
}
