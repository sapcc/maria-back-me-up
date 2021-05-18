package error

type (
	// DatabaseMissingError error for missing database
	DatabaseMissingError struct {
		message string
	}
	// DatabaseConnectionError error for dbconnection issues
	DatabaseConnectionError struct {
		message string
	}

	// DatabaseNoTablesError error when database is empty
	DatabaseNoTablesError struct {
		message string
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
