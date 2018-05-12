package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	btAll         = "all"
	hnAuthz       = "Authorization"
	hnContentType = "Content-Type"
	hvJSON        = "application/json"
	apiPath       = "b2api/v1"
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
	file        string
	buffer      int
}

var (
	debugL = log.New(os.Stdout, "DEBUG: ", log.LstdFlags)
	errL   = log.New(os.Stdout, "  ERR: ", log.LstdFlags)
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
	flag.StringVar(&options.file, "path", "", "File to upload.")
	flag.IntVar(&options.buffer, "buffer", 1024*1024*128, "Read file buffer size.")
	flag.Parse()

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
	err = upload(b2)
	if err != nil {
		errL.Panicf("Cannot upload: %v", err)
	}
}

func fmtB(bytes int64) string {
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
	} else {
		return fmt.Sprintf("%7d B", bytes)
	}
}

func upload(b2 *B2) error {
	err := b2.AuthorizeAccount()
	if err != nil {
		return err
	}
	err = b2.ListBuckets()
	if err != nil {
		return err
	}
	buckets := make(map[string]*Bucket, len(b2.ListBucketsResp.Buckets))
	for _, b := range b2.ListBucketsResp.Buckets {
		buckets[b.BucketName] = &b
	}
	bucket, ok := buckets[options.bucket]
	if !ok {
		return fmt.Errorf("Cannot find bucket: %s\n%q", options.bucket, buckets)
	}
	log.Printf("bucket = %#v", bucket)

	bufferSize := options.buffer
	fr, err := OpenFileReader(options.file, bufferSize)
	if err != nil {
		return err
	}
	defer fr.Close()

	workers := 16
	uploads := make(chan FileChunk, workers*2)
	go func() {
		ticker := time.NewTicker(time.Second * 5)
		defer ticker.Stop()
		var stats FileStats
		for {
			select {
			case chunk, ok := <-uploads:
				if !ok {
					debugL.Printf("trace: uploads closed")
					ticker.Stop()
				}
				if chunk.FileStats.Offset > stats.Offset {
					stats = chunk.FileStats
				}
			case _, ok := <-ticker.C:
				if !ok {
					debugL.Printf("trace: handle ticker closed")
					return
				}
				if stats.Offset > 0 {
					debugL.Printf("Uploaded: %s %s %5.2f%%", stats.Path, fmtB(stats.Offset), float64(stats.Offset)/float64(fr.Size)*100)
				}
			}
		}
		debugL.Printf("trace: logger exit")
	}()

	chunks := make(chan FileChunk, fr.Size/int64(fr.BufferSize))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			err := b2.GetUploadPartURL()
			if err != nil {
				errL.Panicf("Unable to get upload url: thread_id=%d: %v", tid, err)
			}
			for {
				select {
				case chunk, ok := <-chunks:
					if !ok {
						debugL.Printf("trace: handle chunks closed")
						return
					}
					err := b2.UploadPart()
					if err != nil {
						errL.Panicf("Unable to upload chunk: thread_id=%d part_number=%d: %v", tid, chunk.Number, err)
					}
					uploads <- chunk
				}
			}
			debugL.Printf("trace: worker:%d exit", tid)
		}(i)
	}

	err = fr.Read(chunks)
	if err != nil {
		return err
	}
	debugL.Printf("trace: chunks read")
	close(chunks)
	debugL.Printf("trace: chunks closed")

	wg.Wait()
	debugL.Printf("trace: workers complete")
	close(uploads)
	debugL.Printf("trace: uploads closed")

	return nil
}

func newB2() (*B2, error) {
	return &B2{
		client: &http.Client{},
		apiURL: options.api,
	}, nil
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

// b2 is the backblaze client
type B2 struct {
	client *http.Client
	apiURL string

	AuthorizeAccountResp struct {
		AbsoluteMinimumPartSize int    `json:"absoluteMinimumPartSize"`
		AccountID               string `json:"accountId"`
		APIURL                  string `json:"apiUrl"`
		AuthorizationToken      string `json:"authorizationToken"`
		DownloadURL             string `json:"downloadUrl"`
		MinimumPartSize         int    `json:"minimumPartSize"`
		RecommendedPartSize     int    `json:"recommendedPartSize"`
	}
	ListBucketsReq struct {
		AccountID   string   `json:"accountId"`
		BucketTypes []string `json:"bucketTypes,omitempty"`
	}
	ListBucketsResp struct {
		Buckets []Bucket `json:"buckets"`
	}
	StartLargeFileResp struct{}
	StartLargeFileReq  struct {
		BucketID    string `json:"bucketId"`
		FileName    string `json:"fileName"`
		ContentType string `json:"contentType"`
	}
}

// AuthorizeAccount
func (b2 *B2) AuthorizeAccount() error {
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
	debugL.Printf("%s:%s:%d %s", http.MethodGet, b2.url("b2_authorize_account"), httpResp.StatusCode, httpResp.Status)
	defer httpResp.Body.Close()
	out, err := ioutil.ReadAll(httpResp.Body)
	if err != nil {
		return err
	}
	debugL.Printf("%s:%s:%s", http.MethodGet, b2.url("b2_authorize_account"), out)
	err = json.Unmarshal(out, &b2.AuthorizeAccountResp)
	if err != nil {
		return err
	}
	b2.apiURL = b2.AuthorizeAccountResp.APIURL
	return nil
}

func (b2 *B2) GetUploadPartURL() error {
	return nil
}

func (b2 *B2) ListBuckets() error {
	b2.ListBucketsReq.AccountID = b2.AuthorizeAccountResp.AccountID
	b2.ListBucketsReq.BucketTypes = []string{btAll}
	return b2.doRequest(http.MethodPost, "b2_list_buckets", b2.ListBucketsReq, &b2.ListBucketsResp)
}

func (b2 *B2) StartLargeFile(bucket *Bucket) error {
	b2.StartLargeFileReq.BucketID = bucket.BucketID
	b2.StartLargeFileReq.ContentType = options.contentType
	b2.StartLargeFileReq.FileName = options.file
	return b2.doRequest(http.MethodPost, "b2_start_large_file", &b2.StartLargeFileReq, &b2.StartLargeFileResp)
}

func (b2 *B2) UploadPart() error {
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
		debugL.Printf("%s:%d", id(), len(in))
		r = bytes.NewReader(in)
	}
	httpReq, err := http.NewRequest(method, b2.url(endpoint), r)
	httpReq.Header.Add(hnAuthz, b2.AuthorizeAccountResp.AuthorizationToken)
	httpReq.Header.Add(hnContentType, hvJSON)
	httpResp, err := b2.client.Do(httpReq)
	if err != nil {
		return err
	}
	debugL.Printf("%s:%d %s", id(), httpResp.StatusCode, httpResp.Status)
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

// OpenFileReader opens a file reader for a given file with a number of bytes buffer size.
func OpenFileReader(path string, bufferSize int) (*FileReader, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &FileReader{
		BufferSize:       bufferSize,
		Path:             path,
		Size:             fi.Size(),
		file:             file,
		logEveryDuration: time.Second * 3,
	}, nil
}

// FileChunk represents a set of bytes read from the file.
type FileChunk struct {
	Bytes  []byte
	Length int
	Number int
	FileStats
}

// FileStats represent a snapshot of reader stats.
type FileStats struct {
	Offset int64
	Path   string
	Start  time.Time
}

// FileReader batches bytes into numbered chunks.
type FileReader struct {
	BufferSize int
	Size       int64
	Offset     int64
	Path       string

	file             *os.File
	logEveryDuration time.Duration
}

func (fr *FileReader) Read(chunk chan<- FileChunk) error {
	buffer := make([]byte, fr.BufferSize)
	bytesRead := 0
	var err error
	number := 0
	var chunkStart time.Time
	for {
		chunkStart = time.Now()
		bytesRead, err = fr.file.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Panicf("err reading file: %s:%d %v", fr.Path, fr.Offset, err)
		}
		fr.Offset += int64(bytesRead)
		number++
		chunk <- FileChunk{
			Bytes:  buffer,
			Length: bytesRead,
			Number: number,
			FileStats: FileStats{
				Offset: fr.Offset,
				Path:   fr.Path,
				Start:  chunkStart,
			}}
	}
	if bytesRead > 0 {
		fr.Offset += int64(bytesRead)
		number++
		chunk <- FileChunk{
			Bytes:  buffer,
			Length: bytesRead,
			Number: number,
			FileStats: FileStats{
				Offset: fr.Offset,
				Path:   fr.Path,
				Start:  chunkStart,
			}}
	}
	return nil
}

// Close closes the file reader.
func (fr *FileReader) Close() error {
	return fr.file.Close()
}
