package b2

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"lfu"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cheggaaa/pb"
)

const (
	apiPath         = "b2api/v1"
	btAll           = "all"
	hnAuthz         = "Authorization"
	hnPart          = "X-Bz-Part-Number"
	hnContentType   = "Content-Type"
	hnContentLength = "Content-Length"
	hnSha1          = "X-Bz-Content-Sha1"
	hvJSON          = "application/json"
)

// Backblaze options.
type Options struct {
	AccountID     string // Backblaze account id.
	APIURL        string // Backblaze api endpoint.
	ApplicationID string // Backblaze application id.
	Batch         int    // Number of bytes to batch on read.
	Bucket        string // Target bucket to upload into.
	Buffer        int    // Number of batches to queue on read.
	Workers       int    // Number of concurrent uploads.
}

var (
	debugL = log.New(ioutil.Discard, "DEBUG", log.LstdFlags)
	traceL = log.New(ioutil.Discard, "TRACE", log.LstdFlags)
	errL   = log.New(ioutil.Discard, "ERROR", log.LstdFlags)
)

func Upload(rpath, lpath string, opts *Options) error {
	// todo move bucket from option to top level argument
	bucket, batchSize, bufferSize, workers := opts.Bucket, opts.Batch, opts.Buffer, opts.Workers
	c := &client{
		hc: &http.Client{},
		authn: authn{
			apiURL:        opts.APIURL,
			accountID:     opts.AccountID,
			applicationID: opts.ApplicationID,
		},
		session: session{
			apiURL:     "",
			authzToken: "",
		},
	}

	aa := &AuthorizeAccount{}
	err := aa.Do(c)
	if err != nil {
		return err
	}

	lb := &ListBuckets{}
	lb.Req.AccountID = aa.Resp.AccountID
	lb.Req.BucketTypes = []string{btAll}
	err = lb.Do(c)
	if err != nil {
		return err
	}
	b := lb.Get(bucket)
	if b == nil {
		return fmt.Errorf("Cannot find bucket: %s", bucket)
	}
	debugL.Printf("bucket=%#v", b)

	lw, err := lfu.OpenLogWriter(lpath, 1024*1024)
	if err != nil {
		return err
	}
	defer lw.Close()

	fr, err := lfu.OpenFileReader(rpath, batchSize)
	if err != nil {
		return err
	}
	defer fr.Close()

	slf := &StartLargeFile{}
	slf.Req.BucketID = b.BucketID
	slf.Req.ContentType = fr.ContentType
	slf.Req.FileName = rpath
	err = slf.Do(c)
	if err != nil {
		return err
	}

	prefix := fmt.Sprintf("%s x %d (%s) %s ",
		lfu.FmtB(int64(batchSize)),
		bufferSize,
		strings.TrimSpace(lfu.FmtB(fr.Size)),
		filepath.Base(rpath))
	width := 120
	bar := pb.New64(fr.Size).
		Prefix(prefix).
		Set64(lw.Offset()).
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

	chunks := make(chan lfu.FileChunk, bufferSize*workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()

			gupu := &GetUploadPartURL{}
			gupu.Req.FileID = slf.Resp.FileID
			err := gupu.Do(c)
			if err != nil {
				errL.Panicf("unable to get upload url: worker_id=%d: %v", wid, err)
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
					err := up.Do(c)
					if err != nil {
						errL.Panicf("Unable to upload chunk: worker_id=%d part_number=%d: %v", wid, chunk.Number, err)
					}
					lw.Append(&chunk)
					bar.Add(chunk.Length)
				}
			}
		}(i)
	}

	err = fr.Read(chunks, lw.Offset())
	if err != nil {
		return err
	}
	wg.Wait()

	flf := &FinishLargeFile{}
	flf.Req.FileID = slf.Resp.FileID
	flf.Req.SHA1, err = lw.ToStrings()
	if err != nil {
		return err
	}
	err = flf.Do(c)
	if err != nil {
		return err
	}

	return nil
}

type session struct {
	apiURL     string // session api url (see authorize)
	authzToken string // session authz token (see authorize)
}

type authn struct {
	apiURL        string // authentication api url
	accountID     string // account id
	applicationID string // application id
}

type client struct {
	hc *http.Client
	session
	authn
}

// url builds a url string for the rpc call
func (c *client) url(rpc string) string {
	if rpc == "b2_authorize_account" {
		// authorization starts at a static endpoint, the session urls are 'dynamic'
		return fmt.Sprintf("%s/%s/%s", c.authn.apiURL, apiPath, rpc)
	}
	return fmt.Sprintf("%s/%s/%s", c.session.apiURL, apiPath, rpc)
}

// doRequest issues the http request to the endpoint serializing json from req and to resp
func (c *client) doRequest(method string, endpoint string, req interface{}, resp interface{}) error {
	id := func() string { return fmt.Sprintf("%s: %s", method, c.url(endpoint)) }
	var r io.Reader
	if req != nil {
		in, err := json.Marshal(req)
		if err != nil {
			return err
		}
		traceL.Printf("%s:%d", id(), len(in))
		r = bytes.NewReader(in)
	}
	httpReq, err := http.NewRequest(method, c.url(endpoint), r)
	httpReq.Header.Add(hnAuthz, c.authzToken)
	httpReq.Header.Add(hnContentType, hvJSON)
	httpResp, err := c.hc.Do(httpReq)
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
