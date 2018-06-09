package lfu

import (
	"crypto/sha1"
	"encoding/hex"
)

// FileChunk represents a set of bytes read from the file.
type FileChunk struct {
	Bytes  []byte
	Length int
	Number int
	SHA1   string
	FileStats
}

// Sum computes a sha1 hex digest as a string and sets SHA1.
func (fc *FileChunk) Sum() {
	h := sha1.New()
	h.Write(fc.Bytes)
	fc.SHA1 = hex.EncodeToString(h.Sum(nil))
}
