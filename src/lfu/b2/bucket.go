package b2

type Bucket struct {
	AccountID      string            `json:"accountId"`
	BucketID       string            `json:"bucketId"`
	BucketName     string            `json:"bucketName"`
	BucketType     string            `json:"bucketType"`
	BucketInfo     map[string]string `json:"bucketInfo"`
	LifecycleRules []string          `json:"lifecycleRules"`
	Revision       int64             `json:"revision"`
}
