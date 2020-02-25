package lfu

import (
	"crypto/sha1"
	"fmt"
)

// FileChunk represents a set of bytes read from the file.
type FileChunk struct {
	Bytes  []byte
	Length int
	Number int
	Offset int64
	SHA1   []byte
}

// Sum computes a sha1 hex digest as a string and sets SHA1.
func (fc *FileChunk) Sum() {
	h := sha1.New()
	h.Write(fc.Bytes)
	fc.SHA1 = h.Sum(nil)
}

func (fc FileChunk) String() string {
	return fmt.Sprintf("number: %d offset: %d length: %d", fc.Number, fc.Offset, fc.Length)
}
