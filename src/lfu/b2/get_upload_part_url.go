package b2

import "net/http"

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

func (gupu *GetUploadPartURL) Do(c *client) error {
	return c.doRequest(http.MethodPost, "b2_get_upload_part_url", &gupu.Req, &gupu.Resp)
}
