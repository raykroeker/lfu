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
	"os"
	"path/filepath"
	"strings"
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
}

var (
	debugL = log.New(os.Stdout, "DEBUG ", log.LstdFlags)
	traceL = log.New(ioutil.Discard, "TRACE ", log.LstdFlags)
	errL   = log.New(os.Stdout, "ERROR ", log.LstdFlags)
)

func Upload(rpath, lpath string, opts *Options) error {
	// todo move bucket from option to top level argument
	bucket, batchSize, bufferSize := opts.Bucket, opts.Batch, opts.Buffer
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

	// slf := &StartLargeFile{}
	// slf.Req.BucketID = b.BucketID
	// slf.Req.ContentType = fr.ContentType
	// slf.Req.FileName = rpath
	// err = slf.Do(c)
	// if err != nil {
	// 	return err
	// }

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

	partSize := int64(5 * 1024 * 1024 * 1024)
	partCount := int(fr.Size / partSize)
	parts := make([]*lfu.FilePart, partCount)

	rdir := filepath.Dir(rpath)
	rbase := filepath.Base(rpath)
	for i := 0; i < partCount; i++ {
		n := i + 1
		parts[i] = &lfu.FilePart{
			Path:   filepath.Join(rdir, fmt.Sprintf(".%s-%03d", strings.Split(rbase, ".")[0], n)),
			Number: n,
		}
	}
	debugL.Printf("part size: %s part count: %d", lfu.FmtB(partSize), partCount)

	// a single consumer of file chunks will write to file part then emit the file part on completion
	chunks := make(chan lfu.FileChunk, 2)
	go func() {
		var current, previous *lfu.FilePart
		for {
			select {
			case chunk, ok := <-chunks:
				if !ok {
					debugL.Printf("close part: %s", current)
					err := current.Close()
					if err != nil {
						errL.Panicf("unable to close part: %s: %v", current, err)
					}
					return
				}
				current = parts[fr.Size/chunk.Offset]
				if current != previous {
					if previous != nil {
						debugL.Printf("close part: %s", previous)
						err := previous.Close()
						if err != nil {
							errL.Panicf("unable to close part: %s: %v", previous, err)
						}
					}
					debugL.Printf("open part: %s", current)
					err = current.OpenWriter()
					if err != nil {
						errL.Panicf("unable to open part: %s: %v", current, err)
					}
				}
				err := current.Write(&chunk)
				if err != nil {
					errL.Panicf("unable to write chunk to part: %s: %s: %v", chunk, current, err)
				}
				bar.Add(chunk.Length)
				previous = current
			}
		}
	}()

	err = fr.Read(chunks, lw.Offset())
	if err != nil {
		return err
	}

	// flf := &FinishLargeFile{}
	// flf.Req.FileID = slf.Resp.FileID
	// flf.Req.SHA1, err = lw.ToStrings()
	// if err != nil {
	// 	return err
	// }
	// err = flf.Do(c)
	// if err != nil {
	// 	return err
	// }

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

func (c *client) clone() *client {
	return &client{
		hc:      &http.Client{},
		authn:   c.authn,
		session: c.session,
	}
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
