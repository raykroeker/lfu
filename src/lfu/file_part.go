package lfu

import (
	"fmt"
	"os"
)

type FilePart struct {
	Path   string
	Length int64
	Number int
	Offset int64
	SHA1   []byte

	file *os.File
}

func (fp *FilePart) OpenWriter() error {
	var err error
	fp.file, err = os.OpenFile(fp.Path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	return err
}

func (fp *FilePart) Close() error {
	if fp.file != nil {
		return fp.file.Close()
	}
	return nil
}

func (fp *FilePart) Write(chunk *FileChunk) error {
	return nil
}

func (fp FilePart) String() string {
	return fmt.Sprintf("path: %s number: %d", fp.Path, fp.Number)
}
