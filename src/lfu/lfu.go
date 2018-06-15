package lfu

import "fmt"

// FmtB formats bytes as a human readable constant width string.
func FmtB(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%7d B", bytes)
	} else if bytes < (1024 * 1024) {
		return fmt.Sprintf("%6.1f KiB", float64(bytes)/float64(1024))
	} else if bytes < (1024 * 1024 * 1024) {
		return fmt.Sprintf("%6.1f MiB", float64(bytes)/float64(1024*1024))
	} else if bytes < (1024 * 1024 * 1024 * 1024) {
		return fmt.Sprintf("%6.1f GiB", float64(bytes)/float64(1024*1024*1024))
	} else if bytes < (1024 * 1024 * 1024 * 1024 * 1024) {
		return fmt.Sprintf("%6.1f TiB", float64(bytes)/float64(1024*1024*1024*1024))
	} else if bytes < (1024 * 1024 * 1024 * 1024 * 1024 * 1024) {
		return fmt.Sprintf("%6.1f PiB", float64(bytes)/float64(1024*1024*1024*1024*1024))
	}
	panic(fmt.Sprintf("%d B (overflow int64)", bytes))
}
