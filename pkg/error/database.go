package error

type (
	DatabaseMissingError struct {
		message string
	}

	DatabaseConnectionError struct {
		message string
	}
)

func (d *DatabaseMissingError) Error() string {
	return "database not available"
}

func (d *DatabaseConnectionError) Error() string {
	return "cannot connect to database"
}
