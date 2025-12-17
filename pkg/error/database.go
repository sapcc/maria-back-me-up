// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company
// SPDX-License-Identifier: Apache-2.0

package error

type (
	// DatabaseMissingError error for missing database
	DatabaseMissingError struct {
	}
	// DatabaseConnectionError error for dbconnection issues
	DatabaseConnectionError struct {
	}

	// DatabaseNoTablesError error when database is empty
	DatabaseNoTablesError struct {
	}
)

func (d *DatabaseMissingError) Error() string {
	return "database not available"
}

func (d *DatabaseConnectionError) Error() string {
	return "cannot connect to database"
}

func (d *DatabaseNoTablesError) Error() string {
	return "database has no tables"
}
