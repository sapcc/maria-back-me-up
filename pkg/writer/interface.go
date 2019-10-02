package writer

import "bufio"

type WriteString interface {
	Write(f string, r *bufio.Reader) error
	CleanupOldBackups() error
}
