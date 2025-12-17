// SPDX-FileCopyrightText: 2019 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

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
)
