package b2

import "net/http"

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

func (slf *StartLargeFile) Do(c *client) error {
	return c.doRequest(http.MethodPost, "b2_start_large_file", &slf.Req, &slf.Resp)
}
