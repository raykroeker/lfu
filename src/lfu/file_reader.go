package lfu

import (
	"io"
	"net/http"
	"os"
	"time"
)

// FileReader batches bytes into numbered chunks.
type FileReader struct {
	ChunkSize   int
	ContentType string
	Size        int64
	Offset      int64
	Path        string

	file             *os.File
	logEveryDuration time.Duration
}

// OpenFileReader opens a FileReader for a given file with a number of bytes chunk size.
func OpenFileReader(path string, chunkSize int) (*FileReader, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 512)
	_, err = file.Read(buf)
	if err != nil {
		return nil, err
	}
	_, err = file.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, err
	}
	return &FileReader{
		ChunkSize:        chunkSize,
		ContentType:      http.DetectContentType(buf),
		Path:             path,
		Size:             fi.Size(),
		file:             file,
		logEveryDuration: time.Second * 3,
	}, nil
}

// Read collects chunks from disk and emits them onto the channel and closes on return.
func (fr *FileReader) Read(ch chan<- FileChunk, offset int64) error {
	defer close(ch)

	var err error
	fr.Offset, err = fr.file.Seek(offset, os.SEEK_SET)
	if err != nil {
		return err
	}

	buffer := make([]byte, fr.ChunkSize)
	bytesRead := 0
	number := 0
	for {
		bytesRead, err := fr.file.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		fr.Offset += int64(bytesRead)
		number++
		ch <- FileChunk{
			Bytes:  buffer,
			Length: bytesRead,
			Offset: fr.Offset,
			Number: number,
		}
		buffer = make([]byte, fr.ChunkSize)
	}
	if bytesRead > 0 {
		fr.Offset += int64(bytesRead)
		number++
		ch <- FileChunk{
			Bytes:  buffer,
			Length: bytesRead,
			Number: number,
			Offset: fr.Offset,
		}
		buffer = nil
	}
	return nil
}

// Close closes the file reader.
func (fr *FileReader) Close() error {
	return fr.file.Close()
}
