// https://www.backblaze.com/b2/docs/large_files.html
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"lfu"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	btAll           = "all"
	hnAuthz         = "Authorization"
	hnPart          = "X-Bz-Part-Number"
	hnContentType   = "Content-Type"
	hnContentLength = "Content-Length"
	hnSha1          = "X-Bz-Content-Sha1"
	hvJSON          = "application/json"
	apiPath         = "b2api/v1"
)

var environment struct {
	authn struct {
		accountID     string
		applicationID string
	}
}

var options struct {
	api         string
	bucket      string
	contentType string
	debug       bool
	trace       bool
	file        string
	batch       int
	buffer      int
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
	flag.StringVar(&options.contentType, "content-type", "", "The file content (mime) type.")
	flag.BoolVar(&options.debug, "debug", false, "Enable debug logging.")
	flag.BoolVar(&options.trace, "trace", false, "Enable trace logging.")
	flag.StringVar(&options.file, "path", "", "File to upload.")
	flag.IntVar(&options.batch, "batch", 1024*1024*128, "Read file batch (bytes) size.")
	flag.IntVar(&options.buffer, "buffer", 1, "Read file buffer (batches to queue) size.")
	flag.Parse()

	if options.debug {
		debugL.SetOutput(os.Stdout)
	}
	if options.trace {
		debugL.SetOutput(os.Stdout)
		traceL.SetOutput(os.Stdout)
	}

	if options.bucket == "" || options.file == "" || options.contentType == "" {
		flag.Usage()
		os.Exit(1)
	}
	if environment.authn.accountID == "" || environment.authn.applicationID == "" {
		flag.Usage()
		os.Exit(1)
	}

	b2, err := newB2()
	if err != nil {
		errL.Panicf("Cannot initialize: %v", err)
	}
	defer b2.Close()

	start := time.Now()
	defer func() {
		debugL.Printf("Duration: %s", time.Since(start))
	}()
	// err = upload(b2)
	// if err != nil {
	// 	errL.Panicf("Cannot upload: %v", err)
	// }
	w := filepath.Join(filepath.Dir(options.file), "f")
	r := options.file
	err = Copy(r, w, &CopyOpts{
		Resume:    false,
		Overwrite: false,
		Batch:     options.batch,
		Buffer:    options.buffer,
	})
	if err != nil {
		errL.Panicf("Cannot copy: %v", err)
	}
}

func upload(b2 *B2) error {
	aa := &AuthorizeAccount{}
	err := aa.Do(b2)
	if err != nil {
		return err
	}

	lb := &ListBuckets{}
	lb.Req.AccountID = aa.Resp.AccountID
	lb.Req.BucketTypes = []string{btAll}
	err = lb.Do(b2)
	if err != nil {
		return err
	}
	b := lb.Get(options.bucket)
	if b == nil {
		return fmt.Errorf("Cannot find bucket: %s", options.bucket)
	}
	debugL.Printf("bucket=%#v", b)

	slf := &StartLargeFile{}
	slf.Req.BucketID = b.BucketID
	slf.Req.ContentType = options.contentType
	slf.Req.FileName = options.file
	err = slf.Do(b2)
	if err != nil {
		return err
	}

	bufferSize := options.buffer
	fr, err := lfu.OpenFileReader(options.file, bufferSize)
	if err != nil {
		return err
	}
	defer fr.Close()

	log, err := OpenLog(options.file, fr.Size, int64(bufferSize))
	if err != nil {
		return err
	}
	defer func() {
		err := log.Close()
		if err != nil {
			errL.Printf("Cannot close log: %v", err)
		}
	}()

	workers := 16
	uploads := make(chan lfu.FileChunk, workers*2)
	go func() {
		ticker := time.NewTicker(time.Second * 5)
		defer ticker.Stop()
		var stats lfu.FileStats
		for {
			select {
			case chunk, ok := <-uploads:
				if !ok {
					traceL.Printf("uploads closed")
					ticker.Stop()
				}
				err := log.Append(&chunk)
				if err != nil {
					errL.Panicf("Cannot append chunk to log: %v", err)
				}
				if chunk.FileStats.Offset > stats.Offset {
					stats = chunk.FileStats
				}
			case _, ok := <-ticker.C:
				if !ok {
					traceL.Printf("ticker stopped (closed)")
					return
				}
				if stats.Offset > 0 {
					debugL.Printf("Uploaded: %s %s %5.2f%%", stats.Path, FmtB(stats.Offset), float64(stats.Offset)/float64(fr.Size)*100)
				}
			}
		}
	}()

	chunks := make(chan lfu.FileChunk, fr.Size/int64(fr.ChunkSize))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()

			gupu := &GetUploadPartURL{}
			gupu.Req.FileID = slf.Resp.FileID
			err := gupu.Do(b2)
			if err != nil {
				errL.Panicf("unable to get upload url: thread_id=%d: %v", tid, err)
			}
			for {
				select {
				case chunk, ok := <-chunks:
					if !ok {
						traceL.Printf("chunks closed")
						return
					}
					chunk.Sum()

					up := &UploadPart{}
					up.GetUploadPartURL = gupu
					up.FileChunk = &chunk
					err := up.Do(b2)
					if err != nil {
						errL.Panicf("Unable to upload chunk: thread_id=%d part_number=%d: %v", tid, chunk.Number, err)
					}

					uploads <- chunk
				}
			}
		}(i)
	}

	err = fr.Read(chunks, 0)
	if err != nil {
		return err
	}
	traceL.Printf("file read (%s)", FmtB(fr.Size))
	close(chunks)

	wg.Wait()
	close(uploads)

	flf := &FinishLargeFile{}
	flf.Req.FileID = slf.Resp.FileID
	flf.Req.SHA1, err = log.ToStrings()
	if err != nil {
		return err
	}
	err = flf.Do(b2)
	if err != nil {
		return err
	}

	return nil
}

func newB2() (*B2, error) {
	return &B2{
		client: &http.Client{},
		apiURL: options.api,
	}, nil
}

// B2 is the backblaze client
type B2 struct {
	client     *http.Client
	apiURL     string
	authzToken string
}

type Bucket struct {
	AccountID      string            `json:"accountId"`
	BucketID       string            `json:"bucketId"`
	BucketName     string            `json:"bucketName"`
	BucketType     string            `json:"bucketType"`
	BucketInfo     map[string]string `json:"bucketInfo"`
	LifecycleRules []string          `json:"lifecycleRules"`
	Revision       int64             `json:"revision"`
}

// AuthorizeAccount https://www.backblaze.com/b2/docs/b2_authorize_account.html
type AuthorizeAccount struct {
	Req  struct{}
	Resp struct {
		AbsoluteMinimumPartSize int    `json:"absoluteMinimumPartSize"`
		AccountID               string `json:"accountId"`
		APIURL                  string `json:"apiUrl"`
		AuthorizationToken      string `json:"authorizationToken"`
		DownloadURL             string `json:"downloadUrl"`
		MinimumPartSize         int    `json:"minimumPartSize"`
		RecommendedPartSize     int    `json:"recommendedPartSize"`
	}
}

func (aa *AuthorizeAccount) Do(b2 *B2) error {
	httpReq, err := http.NewRequest(http.MethodGet, b2.url("b2_authorize_account"), nil)
	if err != nil {
		return err
	}
	httpReq.Header.Add(hnContentType, hvJSON)
	httpReq.SetBasicAuth(environment.authn.accountID, environment.authn.applicationID)
	httpResp, err := b2.client.Do(httpReq)
	if err != nil {
		return err
	}
	traceL.Printf("%s:%s:%d %s", http.MethodGet, b2.url("b2_authorize_account"), httpResp.StatusCode, httpResp.Status)
	defer httpResp.Body.Close()
	out, err := ioutil.ReadAll(httpResp.Body)
	if err != nil {
		return err
	}
	traceL.Printf("%s:%s:%s", http.MethodGet, b2.url("b2_authorize_account"), out)
	err = json.Unmarshal(out, &aa.Resp)
	if err != nil {
		return err
	}
	b2.apiURL = aa.Resp.APIURL
	b2.authzToken = aa.Resp.AuthorizationToken
	return nil
}

type FinishLargeFile struct {
	Req struct {
		FileID string   `json:"fileId"`
		SHA1   []string `json:"partSha1Array"`
	}
	Resp struct {
		FileID          string                 `json:"fileId"`
		FileName        string                 `json:"fileName"`
		AccountID       string                 `json:"accountId"`
		BucketID        string                 `json:"bucketId"`
		ContentLength   string                 `json:"contentLength"`
		ContentSHA1     string                 `json:"contentSha1"`
		ContentType     string                 `json:"contentType"`
		FileInfo        map[string]interface{} `json:"fileInfo"`
		Action          string                 `json:"action"`
		UploadTimestamp uint64                 `json:"uploadTimestamp"`
	}
}

func (flf *FinishLargeFile) Do(b2 *B2) error {
	return b2.doRequest(http.MethodPost, "b2_finish_large_file", &flf.Req, &flf.Resp)
}

type GetUploadPartURL struct {
	Req struct {
		FileID string `json:"fileId"`
	}
	Resp struct {
		FileID             string `json:"fileId"`
		UploadURL          string `json:"uploadUrl"`
		AuthorizationToken string `json:"authorizationToken"`
	}
}

func (gupu *GetUploadPartURL) Do(b2 *B2) error {
	return b2.doRequest(http.MethodPost, "b2_get_upload_part_url", &gupu.Req, &gupu.Resp)
}

type ListBuckets struct {
	Req struct {
		AccountID   string   `json:"accountId"`
		BucketTypes []string `json:"bucketTypes,omitempty"`
	}
	Resp struct {
		Buckets []Bucket `json:"buckets"`
	}

	buckets map[string]*Bucket
}

func (lb *ListBuckets) Do(b2 *B2) error {
	err := b2.doRequest(http.MethodPost, "b2_list_buckets", &lb.Req, &lb.Resp)
	if err != nil {
		return err
	}
	lb.buckets = make(map[string]*Bucket, len(lb.Resp.Buckets))
	for _, b := range lb.Resp.Buckets {
		lb.buckets[b.BucketName] = &b
	}
	return nil
}

func (lb *ListBuckets) Get(name string) *Bucket {
	return lb.buckets[name]
}

// StartLargeFile https://www.backblaze.com/b2/docs/b2_start_large_file.html
type StartLargeFile struct {
	Req struct {
		BucketID    string `json:"bucketId"`
		FileName    string `json:"fileName"`
		ContentType string `json:"contentType"`
	}
	Resp struct {
		FileID          string                 `json:"fileId"`
		FileName        string                 `json:"fileName"`
		AccountID       string                 `json:"accountID"`
		BucketID        string                 `json:"bucketID"`
		ContentType     string                 `json:"contentType"`
		FileInfo        map[string]interface{} `json:"fileInfo"`
		UploadTimestamp uint64                 `json:"uploadTimestamp"`
	}
}

func (slf *StartLargeFile) Do(b2 *B2) error {
	return b2.doRequest(http.MethodPost, "b2_start_large_file", &slf.Req, &slf.Resp)
}

// UploadPart https://www.backblaze.com/b2/docs/b2_upload_part.html
type UploadPart struct {
	Req struct{}
	*GetUploadPartURL
	*lfu.FileChunk
	Resp struct {
		FileID        string `json:"fileId"`
		PartNumber    string `json:"partNumber"`
		ContentLength int    `json:"contentLength"`
		ContentSHA1   string `json:"contentSha1"`
	}
}

func (up *UploadPart) Do(b2 *B2) error {
	id := func() string {
		return fmt.Sprintf("%s: %s: %s", http.MethodPost, up.GetUploadPartURL.Resp.UploadURL, up.FileChunk.SHA1)
	}
	httpReq, err := http.NewRequest(http.MethodPost, up.GetUploadPartURL.Resp.UploadURL, bytes.NewReader(up.FileChunk.Bytes))
	if err != nil {
		return err
	}
	httpReq.Header.Add(hnAuthz, up.GetUploadPartURL.Resp.AuthorizationToken)
	httpReq.Header.Add(hnPart, fmt.Sprintf("%d", up.FileChunk.Number))
	httpReq.Header.Add(hnContentLength, fmt.Sprintf("%d", up.FileChunk.Length))
	httpReq.Header.Add(hnSha1, up.FileChunk.SHA1)
	httpResp, err := b2.client.Do(httpReq)
	if err != nil {
		return err
	}
	traceL.Printf("%s:%d %s", id(), httpResp.StatusCode, httpResp.Status)
	defer httpResp.Body.Close()
	out, err := ioutil.ReadAll(httpResp.Body)
	if err != nil {
		return err
	}
	err = json.Unmarshal(out, &up.Resp)
	if err != nil {
		return err
	}
	if httpResp.StatusCode > 399 {
		errL.Printf("%s:%d %s", id(), httpResp.StatusCode, httpResp.Status)
		errL.Printf("%s", out)
		return fmt.Errorf("%s:%d %s", id(), httpResp.StatusCode, httpResp.Status)
	}
	return nil
}

// Close
func (b2 *B2) Close() {
}

func (b2 *B2) doRequest(method string, endpoint string, req interface{}, resp interface{}) error {
	id := func() string { return fmt.Sprintf("%s: %s", method, b2.url(endpoint)) }
	var r io.Reader
	if req != nil {
		in, err := json.Marshal(req)
		if err != nil {
			return err
		}
		traceL.Printf("%s:%d", id(), len(in))
		r = bytes.NewReader(in)
	}
	httpReq, err := http.NewRequest(method, b2.url(endpoint), r)
	httpReq.Header.Add(hnAuthz, b2.authzToken)
	httpReq.Header.Add(hnContentType, hvJSON)
	httpResp, err := b2.client.Do(httpReq)
	if err != nil {
		return err
	}
	traceL.Printf("%s:%d %s", id(), httpResp.StatusCode, httpResp.Status)
	if resp != nil {
		defer httpResp.Body.Close()
		out, err := ioutil.ReadAll(httpResp.Body)
		if err != nil {
			return err
		}
		err = json.Unmarshal(out, resp)
		if err != nil {
			return err
		}
		if httpResp.StatusCode > 399 {
			errL.Printf("%s:%d %s", id(), httpResp.StatusCode, httpResp.Status)
			errL.Printf("%s", out)
		}
	}
	return nil
}

func (b2 *B2) url(l string) string {
	return fmt.Sprintf("%s/%s/%s", b2.apiURL, apiPath, l)
}

type Log struct {
	bufferSize int64
	file       *os.File
	fileSize   int64
}

func OpenLog(path string, fileSize int64, bufferSize int64) (*Log, error) {
	logPath := filepath.Join(os.TempDir(), fmt.Sprintf("b2-%s.log", filepath.Base(options.file)))
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_SYNC, 0600)
	if err != nil {
		return nil, err
	}
	return &Log{
		bufferSize: bufferSize,
		file:       file,
		fileSize:   fileSize,
	}, nil
}

// Append the file chunk hash (sha1) to the log
func (l *Log) Append(fc *lfu.FileChunk) error {
	_, err := l.file.WriteString(fmt.Sprintf("%d=%s\n", fc.Number, fc.SHA1))
	if err != nil {
		return err
	}
	return nil
}

func (l *Log) Close() error {
	return os.Remove(l.file.Name())
}

func (l *Log) ToStrings() ([]string, error) {
	_, err := l.file.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, err
	}
	ary := make([]string, l.fileSize/l.bufferSize)
	scanner := bufio.NewScanner(l.file)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		if scanner.Err() != nil {
			return nil, scanner.Err()
		}
		line := strings.Split(scanner.Text(), "=")
		n, err := strconv.Atoi(line[0])
		if err != nil {
			return nil, err
		}
		ary[n] = line[1]
	}
	return ary, nil
}
