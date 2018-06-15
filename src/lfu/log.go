package lfu

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type LogWriter struct {
	bufferSize int64
	file       *os.File
	fileSize   int64
	entries    int
	min        int
	max        int
}

func OpenLogWriter(path string, fileSize int64, bufferSize int64) (*LogWriter, error) {
	// file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_SYNC, 0600)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	return &LogWriter{
		bufferSize: bufferSize,
		file:       file,
		fileSize:   fileSize,
	}, nil
}

// Append the file chunk hash (sha1) to the log
func (l *LogWriter) Append(fc *FileChunk) error {
	_, err := l.file.WriteString(fmt.Sprintf("%d=%s\n", fc.Number, fc.SHA1))
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
	return l.file.Close()
}

func (l *LogWriter) ToStrings() ([]string, error) {
	err := l.file.Sync()
	if err != nil {
		return nil, err
	}
	fmt.Printf("\nmin=%d,max=%d,entries=%d\n", l.min, l.max, l.entries)
	_, err = l.file.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, err
	}
	ary := make([]string, l.entries)
	scanner := bufio.NewScanner(l.file)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		if scanner.Err() != nil {
			return nil, scanner.Err()
		}
		line := strings.Split(scanner.Text(), "=")
		if len(line) < 2 {
			return nil, fmt.Errorf("invalid line: %s", line)
		}
		n, err := strconv.Atoi(line[0])
		if err != nil {
			return nil, err
		}
		if len(ary) < n+1 {
			return nil, fmt.Errorf("invalid line: %d<%d", len(ary), n+1)
		}
		ary[n-1] = line[1]
	}
	return ary, nil
}
