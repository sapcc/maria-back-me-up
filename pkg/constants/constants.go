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

package constants

const (
	PORT               = "8081"
	PORT_METRICS       = "8082"
	POSTGRES           = "postgres"
	MARIADB            = "mariadb"
	INDEX              = "templates/index.html"
	BACKUP             = "templates/backup.html"
	RESTORE            = "templates/restore.html"
	MARIADEPLOYMENT    = "templates/maria_deployment.yaml"
	MARIASERIVCE       = "templates/maria_svc.yaml"
	POSTGRESDEPLOYMENT = "templates/postgres_deployment.yaml"
	POSTGRESSERIVCE    = "templates/postgres_svc.yaml"
	RESTOREFOLDER      = "/restore"
	HARDRESTORE        = "hard"
	SOFTRESTORE        = "soft"
	VERIFYINTERFAL     = 30
)
