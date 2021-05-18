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
	// PORT for api server
	PORT = "8081"
	// PORTMETRICS for metrics server
	PORTMETRICS = "8082"
	// POSTGRES name of a database type
	POSTGRES = "postgres"
	// MARIADB name of a database type
	MARIADB = "mariadb"
	// INDEX path to index.html file
	INDEX = "static/html/templates/index.html"
	// BACKUP path to backup.html file
	BACKUP = "static/html/templates/backup.html"
	// RESTORE path to restore.html file
	RESTORE = "static/html/templates/restore.html"
	// MARIADEPLOYMENT path to maria k8s deployment file
	MARIADEPLOYMENT = "k8s_templates/maria_deployment.yaml"
	// MARIASERIVCE path to maria k8s service file
	MARIASERIVCE = "k8s_templates/maria_svc.yaml"
	// POSTGRESDEPLOYMENT path to postgres k8s deployment file
	POSTGRESDEPLOYMENT = "k8s_postgres_deployment.yaml"
	// POSTGRESSERIVCE path to postgres k8s service file
	POSTGRESSERIVCE = "k8s_postgres_svc.yaml"
	// RESTOREFOLDER path to database restore folder
	RESTOREFOLDER = "/restore"
)
