package lfu

import (
	"bytes"
	"fmt"
	"testing"
)

func TestEncode(t *testing.T) {
	fc := FileChunk{
		Bytes:  []byte("Hello log"),
		Number: 1,
		Offset: 2103,
		Length: 3,
	}
	fc.Sum()
	b := encode(&fc)
	fmt.Printf("%s\n", b)
	number, begin, end, sha1 := decode(b)
	if number != fc.Number {
		t.Fail()
	}
	if begin != fc.Offset {
		t.Fail()
	}
	if end != fc.Offset+int64(fc.Length) {
		t.Fail()
	}
	if !bytes.Equal(sha1, fc.SHA1) {
		t.Fail()
	}
}
