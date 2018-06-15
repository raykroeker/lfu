package b2

import "net/http"

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

func (flf *FinishLargeFile) Do(c *client) error {
	return c.doRequest(http.MethodPost, "b2_finish_large_file", &flf.Req, &flf.Resp)
}
