package main

import (
	"fmt"
	"lfu"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cheggaaa/pb"
)

// CopyOpts.
type CopyOpts struct {
	Resume    bool // Resume partial copy.
	Overwrite bool // Overwrite target/write file.
	Batch     int  // Number of bytes to batch on read.
	Buffer    int  // Number of batches to queue on read.
}

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

// Copy a file from one path to another.
func Copy(rpath, wpath string, opts *CopyOpts) error {
	batchSize, bufferSize := opts.Batch, opts.Buffer

	wff := os.O_WRONLY | os.O_SYNC
	traceL.Printf("set:O_WRONLY | os.O_SYNC")
	_, err := os.Stat(wpath)
	if err != nil {
		if os.IsNotExist(err) {
			traceL.Printf("set:O_CREATE")
			wff |= os.O_CREATE
		}
	} else {
		if opts.Overwrite {
			if !opts.Resume {
				traceL.Printf("set:O_TRUNC")
				wff |= os.O_TRUNC
			}
		} else {
			return fmt.Errorf("file exists: %s", wpath)
		}
	}
	w, err := os.OpenFile(wpath, wff, 0644)
	if err != nil {
		return err
	}
	defer w.Close()

	r, err := lfu.OpenFileReader(rpath, batchSize)
	if err != nil {
		return err
	}
	defer r.Close()

	prefix := fmt.Sprintf("%s x %d (%s) %s ",
		FmtB(int64(opts.Batch)),
		opts.Buffer,
		strings.TrimSpace(FmtB(r.Size)),
		filepath.Base(rpath))
	width := 120
	bar := pb.New64(r.Size).
		Prefix(prefix).
		SetRefreshRate(time.Second * 1).
		SetWidth(width).
		SetMaxWidth(width).
		SetUnits(pb.U_BYTES)
	bar.ShowPercent = true
	bar.ShowCounters = false
	bar.ShowSpeed = false
	bar.ShowTimeLeft = false
	bar.ShowBar = true
	bar.ShowFinalTime = true
	bar.ShowElapsedTime = false
	bar.Start()
	defer bar.Finish()

	done := make(chan bool)
	chunks := make(chan lfu.FileChunk, bufferSize)
	sum := int64(0)
	go func() {
		for {
			select {
			case chunk, ok := <-chunks:
				if !ok {
					traceL.Printf("chunks closed")
					done <- true
					return
				}
				chunk.Sum()
				n, err := w.Write(chunk.Bytes[0:chunk.Length])
				if err != nil {
					errL.Panicf("Could not write: %v", err)
				}
				if n != chunk.Length {
					errL.Panicf("Could not write: %d<>%d", n, chunk.Length)
				}
				sum += int64(n)
				bar.Set64(sum)
				traceL.Printf("%s copied\t%d:%s", FmtB(sum), chunk.Number, chunk.SHA1)
			}
		}
	}()
	debugL.Printf("Copy: %s>%s", rpath, wpath)
	err = r.Read(chunks, 0)
	if err != nil {
		return err
	}
	<-done
	return nil
}
