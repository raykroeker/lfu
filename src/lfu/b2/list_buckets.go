package b2

import "net/http"

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

func (lb *ListBuckets) Do(c *client) error {
	err := c.doRequest(http.MethodPost, "b2_list_buckets", &lb.Req, &lb.Resp)
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
