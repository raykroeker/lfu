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

// Copy a file from one path to another.
func Copy(rpath, wpath, lpath string, opts *CopyOpts) error {
	batchSize, bufferSize, overwrite, resume := opts.Batch, opts.Buffer, opts.Overwrite, opts.Resume

	wff := os.O_WRONLY | os.O_SYNC
	traceL.Printf("set:O_WRONLY | os.O_SYNC")
	_, err := os.Stat(wpath)
	if err != nil {
		if os.IsNotExist(err) {
			traceL.Printf("set:O_CREATE")
			wff |= os.O_CREATE
		}
	} else {
		if overwrite {
			if !resume {
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

	l, err := lfu.OpenLogWriter(lpath, r.Size, int64(bufferSize))
	if err != nil {
		return err
	}
	defer l.Close()

	prefix := fmt.Sprintf("%s x %d (%s) %s ",
		lfu.FmtB(int64(batchSize)),
		bufferSize,
		strings.TrimSpace(lfu.FmtB(r.Size)),
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
				traceL.Printf("%s copied\t%d:%s", lfu.FmtB(sum), chunk.Number, chunk.SHA1)
				l.Append(&chunk)
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
