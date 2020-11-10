package golivyclient

//LivyClient livy客户端类
type LivyClient struct {
	BASEURL string
}

//NewClient 创建一个新的livy客户端对象
func NewClient(baseURL string) *LivyClient {
	c := new(LivyClient)
	c.BASEURL = baseURL
	return c
}

//Default 默认的livy客户端
var Default = &LivyClient{}

//Init 初始化livy客户端
func (c *LivyClient) Init(baseURL string) {
	c.BASEURL = baseURL
}

//NewBatch 创建新的Batch
func (c *LivyClient) NewBatch() *Batch {
	nb := NewBatch(c)
	return nb
}

//NewSession 创建新的Session
func (c *LivyClient) NewSession() *Session {
	return NewSession(c)
}
