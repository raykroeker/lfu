package b2

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"lfu"
	"net/http"
)

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

func (up *UploadPart) Do(c *client) error {
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
	httpResp, err := c.hc.Do(httpReq)
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
