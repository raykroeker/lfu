package lfu

import "time"

// FileStats represent a snapshot of reader stats.
type FileStats struct {
	Offset int64
	Path   string
	Start  time.Time
}
