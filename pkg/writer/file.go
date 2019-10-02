package writer

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
)

type File struct {
	location string
}

func NewFile(location string) (f *File, err error) {
	return &File{
		location: location,
	}, err
}

func (c *File) WriteString(f string, r *bufio.Reader) (err error) {
	outFile, err := os.Create(f)
	defer outFile.Close()
	w := gzip.NewWriter(outFile)
	_, err = r.WriteTo(w)
	w.Close()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	return nil
}

func (s *File) CleanupOldBackups() (err error) {
	return
}
