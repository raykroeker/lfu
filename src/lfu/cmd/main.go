// https://www.backblaze.com/b2/docs/large_files.html
package main

import (
	"flag"
	"io/ioutil"
	"lfu/b2"
	"log"
	"os"
	"path/filepath"
	"time"
)

var environment struct {
	authn struct {
		accountID     string
		applicationID string
	}
}

var options struct {
	api     string
	bucket  string
	debug   bool
	trace   bool
	file    string
	batch   int
	buffer  int
	workers int
}

var (
	debugL = log.New(ioutil.Discard, "DEBUG: ", log.LstdFlags)
	errL   = log.New(os.Stderr, "  ERR: ", log.LstdFlags)
	traceL = log.New(ioutil.Discard, "TRACE: ", log.LstdFlags)
)

func init() {
	environment.authn.accountID = os.Getenv("B2_ACCOUNT_ID")
	environment.authn.applicationID = os.Getenv("B2_APPLICATION_ID")
}

func main() {
	flag.StringVar(&options.api, "api", "https://api.backblazeb2.com", "API Endpoint")
	flag.StringVar(&options.bucket, "bucket", "", "The bucket into which the file will be uploaded.")
	flag.BoolVar(&options.debug, "debug", false, "Enable debug logging.")
	flag.BoolVar(&options.trace, "trace", false, "Enable trace logging.")
	flag.StringVar(&options.file, "path", "", "File to upload.")
	flag.IntVar(&options.batch, "batch", 1024*1024*128, "Read file batch (bytes) size.")
	flag.IntVar(&options.buffer, "buffer", 2, "Read file buffer (batches to queue) size.")
	flag.IntVar(&options.workers, "workers", 1, "Concurrent workers.")
	flag.Parse()

	if options.debug {
		debugL.SetOutput(os.Stdout)
	}
	if options.trace {
		debugL.SetOutput(os.Stdout)
		traceL.SetOutput(os.Stdout)
	}

	if options.bucket == "" || options.file == "" {
		flag.Usage()
		os.Exit(1)
	}
	if environment.authn.accountID == "" || environment.authn.applicationID == "" {
		flag.Usage()
		os.Exit(1)
	}

	start := time.Now()
	defer func() {
		debugL.Printf("Duration: %s", time.Since(start))
	}()

	// w := filepath.Join(filepath.Dir(options.file), "f")
	l := filepath.Join(filepath.Dir(options.file), "l")
	r := options.file
	var err error
	// err = Copy(r, w, l, &CopyOpts{
	// 	Resume:    false,
	// 	Overwrite: false,
	// 	Batch:     options.batch,
	// 	Buffer:    options.buffer,
	// })
	err = b2.Upload(r, l, &b2.Options{
		AccountID:     environment.authn.accountID,
		APIURL:        options.api,
		ApplicationID: environment.authn.applicationID,
		Batch:         options.batch,
		Bucket:        options.bucket,
		Buffer:        options.buffer,
	})
	if err != nil {
		errL.Panicf("Cannot upload: %v", err)
	}
}
