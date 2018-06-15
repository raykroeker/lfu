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
func (fr *FileReader) Read(ch chan<- FileChunk, offsetN int) error {
	defer close(ch)

	_, err := fr.file.Seek(int64(offsetN*fr.ChunkSize), os.SEEK_SET)
	if err != nil {
		return err
	}

	buffer := make([]byte, fr.ChunkSize)
	bytesRead := 0
	number := offsetN
	var chunkStart time.Time
	for {
		chunkStart = time.Now()
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
			Number: number,
			FileStats: FileStats{
				Offset: fr.Offset,
				Path:   fr.Path,
				Start:  chunkStart,
			}}
		buffer = make([]byte, fr.ChunkSize)
	}
	if bytesRead > 0 {
		fr.Offset += int64(bytesRead)
		number++
		ch <- FileChunk{
			Bytes:  buffer,
			Length: bytesRead,
			Number: number,
			FileStats: FileStats{
				Offset: fr.Offset,
				Path:   fr.Path,
				Start:  chunkStart,
			}}
		buffer = nil
	}
	return nil
}

// Close closes the file reader.
func (fr *FileReader) Close() error {
	return fr.file.Close()
}
