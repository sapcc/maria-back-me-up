package storage

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
)

// ZipFolderPath zips a folder
func ZipFolderPath(pathToZip string) (pr *io.PipeReader, err error) {
	dir, err := os.Open(pathToZip)
	if err != nil {
		return
	}
	defer dir.Close()

	// get list of files
	files, err := dir.Readdir(0)
	if err != nil {
		return
	}

	pr, pw := io.Pipe()
	tarfileWriter := tar.NewWriter(pw)
	go func() {
		for _, fileInfo := range files {

			if fileInfo.IsDir() {
				continue
			}

			file, err := os.Open(dir.Name() + string(filepath.Separator) + fileInfo.Name())
			if err != nil {
				return
			}
			defer file.Close()

			// prepare the tar header
			header := new(tar.Header)
			header.Name = filepath.Base(file.Name())
			header.Size = fileInfo.Size()
			header.Mode = int64(fileInfo.Mode())
			header.ModTime = fileInfo.ModTime()

			err = tarfileWriter.WriteHeader(header)
			if err != nil {
				return
			}

			_, err = io.Copy(tarfileWriter, file)
			if err != nil {
				return
			}
		}
		tarfileWriter.Close()
		pw.Close()
	}()
	return
}

// FolderSize calcs a folders size
func FolderSize(path string) (size int64, err error) {
	err = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	size = size / 1024 / 1024
	return size, err
}
