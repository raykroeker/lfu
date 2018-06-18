package lfu

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"io"
	"os"
	"sync"
	"time"
)

const (
	// ^[number][begin][end][sha1]
	logEntrySize = 1 + 4 + 8 + 8 + 20
	marker       = byte('^')
)

type LogWriter struct {
	sync.RWMutex
	file     *os.File
	fileSize int64
	entries  int
	offset   int64
	min      int
	max      int
	writer   *bufio.Writer
}

func OpenLogWriter(path string, buffer int) (*LogWriter, error) {
	// file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_SYNC, 0600)
	fi, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	offset := int64(0)
	if fi != nil && fi.Size() > 0 {
		logOffset := fi.Size() - logEntrySize
		buf := make([]byte, logEntrySize)
		_, err := file.ReadAt(buf, logOffset)
		if err != nil {
			return nil, err
		}
		_, err = file.Seek(logOffset, os.SEEK_SET)
		if err != nil {
			return nil, err
		}
		_, offset, _, _ = decode(buf)
	}

	lw := &LogWriter{
		file:   file,
		offset: offset,
		writer: bufio.NewWriterSize(file, buffer),
	}
	go func() {
		for {
			select {
			case <-time.After(time.Second * 1):
				lw.Lock()
				lw.writer.Flush()
				lw.Unlock()
			}
		}
	}()
	return lw, nil
}

// Append the file chunk hash (sha1) to the log
func (l *LogWriter) Append(fc *FileChunk) error {
	l.Lock()
	defer l.Unlock()

	_, err := l.writer.Write(encode(fc))
	if err != nil {
		return err
	}
	if fc.Number < l.min {
		l.min = fc.Number
	}
	if fc.Number > l.max {
		l.max = fc.Number
	}
	l.entries++
	return nil
}

func (l *LogWriter) Close() error {
	l.Lock()
	defer l.Unlock()

	if err := l.writer.Flush(); err != nil {
		l.file.Close()
	}
	return l.file.Close()
}

func (l *LogWriter) Offset() int64 {
	return l.offset
}

func (l *LogWriter) ToStrings() ([]string, error) {
	l.RLock()
	defer l.RUnlock()

	err := l.file.Sync()
	if err != nil {
		return nil, err
	}
	_, err = l.file.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, err
	}
	ary := make([]string, l.entries)
	r := bufio.NewReader(l.file)
	for {
		buf, err := r.ReadBytes(marker)
		if err != nil {
			if io.EOF == err {
				break
			}
			return nil, err
		}
		number, _, _, sha1 := decode(buf)
		ary[number] = hex.EncodeToString(sha1)
	}
	return ary, nil
}

func encode(fc *FileChunk) []byte {
	// 1+4+8+8+20
	b := make([]byte, logEntrySize)
	b[0] = marker
	binary.BigEndian.PutUint32(b[1:], uint32(fc.Number))
	binary.BigEndian.PutUint64(b[5:], uint64(fc.Offset))
	binary.BigEndian.PutUint64(b[13:], uint64(fc.Offset+int64(fc.Length)))
	copy(b[21:], fc.SHA1)
	return b
}

func decode(buf []byte) (int, int64, int64, []byte) {
	// 1+4+8+8+20
	return int(binary.BigEndian.Uint32(buf[1:5])),
		int64(binary.BigEndian.Uint64(buf[5:13])),
		int64(binary.BigEndian.Uint64(buf[13:21])),
		buf[21:]
}
