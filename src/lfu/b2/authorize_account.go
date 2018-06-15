package b2

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
)

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

// Do executes the authorize account sequence using the client.
func (aa *AuthorizeAccount) Do(c *client) error {
	httpReq, err := http.NewRequest(http.MethodGet, c.url("b2_authorize_account"), nil)
	if err != nil {
		return err
	}
	httpReq.Header.Add(hnContentType, hvJSON)
	httpReq.SetBasicAuth(c.authn.accountID, c.authn.applicationID)
	httpResp, err := c.hc.Do(httpReq)
	if err != nil {
		return err
	}
	traceL.Printf("%s:%s:%d %s", http.MethodGet, c.url("b2_authorize_account"), httpResp.StatusCode, httpResp.Status)
	defer httpResp.Body.Close()
	out, err := ioutil.ReadAll(httpResp.Body)
	if err != nil {
		return err
	}
	traceL.Printf("%s:%s:%s", http.MethodGet, c.url("b2_authorize_account"), out)
	err = json.Unmarshal(out, &aa.Resp)
	if err != nil {
		return err
	}
	c.session.apiURL = aa.Resp.APIURL
	c.session.authzToken = aa.Resp.AuthorizationToken
	return nil
}
