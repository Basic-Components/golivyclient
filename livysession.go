package golivyclient

import (
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/Basic-Components/loggerhelper"
)

//Session livy的会话,用于管理交互模式提交的代码
type Session struct {
	Client     *LivyClient            `json:"-"`
	URI        string                 `json:"-"`
	Statements []*Statement           `json:"-"`
	ID         int                    `json:"id"`
	AppID      string                 `json:"appId"`
	Owner      string                 `json:"owner"`
	Kind       string                 `json:"kind"`
	ProxyUser  string                 `json:"proxyUser"`
	AppInfo    map[string]interface{} `json:"appInfo"`
	Log        []string               `json:"log"`
	State      string                 `json:"state"`
}

//NewSessionQuery livy批的创建请求,用于提交固定任务
type NewSessionQuery struct {
	// The name of this session
	Name string `json:"name,omitempty"`
	// The session kind
	Kind string `json:"kind"`
	// User to impersonate when starting the session
	ProxyUser string `json:"proxyUser,omitempty"`
	// jars to be used in this session
	Jars []string `json:"jars,omitempty"`
	// Python files to be used in this session
	PyFiles []string `json:"pyFiles,omitempty"`
	// files to be used in this session
	Files []string `json:"files,omitempty"`
	// Amount of memory to use for the driver process
	DriverMemory string `json:"driverMemory,omitempty"`
	// Number of cores to use for the driver process
	DriverCores int `json:"driverCores,omitempty"`
	// Amount of memory to use per executor process
	ExecutorMemory string `json:"executorMemory,omitempty"`
	// Number of cores to use for each executor
	ExecutorCores int `json:"executorCores,omitempty"`
	// Number of executors to launch for this session
	NumExecutors int `json:"numExecutors,omitempty"`
	// Archives to be used in this session
	Archives []string `json:"archives,omitempty"`
	// The name of the YARN queue to which submitted
	Queue string `json:"queue,omitempty"`
	// Spark configuration properties
	Conf map[string]interface{} `json:"conf,omitempty"`
	// Timeout in second to which session be orphaned
	HeartbeatTimeoutInSecond int `json:"heartbeatTimeoutInSecond,omitempty"`
}

//NewSession 创建新的Session
func NewSession(c *LivyClient) *Session {
	b := new(Session)
	uri := "sessions"
	b.Client = c
	b.URI = uri
	b.Statements = []*Statement{}
	return b
}

//New 创建新的Session请求,并将结果更新到对象自身
func (b *Session) New(q *NewSessionQuery) error {

	url := fmt.Sprintf("%s/%s", b.Client.BASEURL, b.URI)
	resBytes, err := HTTPJSONQuery(url, "POST", q)
	if err != nil {
		return err
	}
	json.Unmarshal(resBytes, b)
	return nil
}

//Copy 克隆一份当前的状态
func (b *Session) Copy() *Session {
	newstates := []*Statement{}

	for _, ele := range b.Statements {
		newstates = append(newstates, ele)
	}

	newlog := []string{}
	for _, ele := range b.Log {
		newlog = append(newlog, ele)
	}

	newAppInfo := map[string]interface{}{}
	for key, value := range b.AppInfo {
		newAppInfo[key] = value
	}

	newone := Session{
		Client:     b.Client,
		URI:        b.URI,
		Statements: newstates,
		ID:         b.ID,
		AppID:      b.AppID,
		Owner:      b.Owner,
		Kind:       b.Kind,
		ProxyUser:  b.ProxyUser,
		AppInfo:    newAppInfo,
		Log:        newlog,
		State:      b.State,
	}
	return &newone
}

//ToJSONString 将消息转未json字符串
func (b *Session) ToJSONString() (string, error) {
	return json.MarshalToString(b)
}

//ToJSON 将消息转未json
func (b *Session) ToJSON() ([]byte, error) {
	return json.Marshal(b)
}

//BytesInfo 获取Session对象的信息
func (b *Session) BytesInfo() ([]byte, error) {
	url := fmt.Sprintf("%s/%s/%d", b.Client.BASEURL, b.URI, b.ID)
	resBytes, err := HTTPJSONQuery(url, "GET")
	if err != nil {
		return nil, err
	}
	return resBytes, nil
}

//Info 获取Session对象的信息
func (b *Session) Info() (*Session, error) {
	resb, err := b.BytesInfo()
	if err != nil {
		return nil, err
	}
	nb := Session{}
	err = json.Unmarshal(resb, &nb)
	if err != nil {
		return nil, err
	}
	nb.Client = b.Client
	nb.URI = b.URI
	b.Statements = []*Statement{}
	return &nb, nil
}

//Update 更新自身
func (b *Session) Update() ([]byte, error) {
	resb, err := b.BytesInfo()
	if err != nil {
		return nil, err
	}
	nb := Session{}
	err = json.Unmarshal(resb, &nb)
	if err != nil {
		return nil, err
	}
	b.ID = nb.ID
	b.AppID = nb.AppID
	b.Owner = nb.Owner
	b.Kind = nb.Kind
	b.ProxyUser = nb.ProxyUser
	b.AppInfo = nb.AppInfo
	b.Log = nb.Log
	b.State = nb.State
	return resb, nil
}

//Close 关闭batch所指向的任务
func (b *Session) Close() error {
	url := fmt.Sprintf("%s/%s/%d", b.Client.BASEURL, b.URI, b.ID)
	_, err := HTTPJSONQuery(url, "DELETE")
	if err != nil {
		return err
	}
	return nil
}

//NewStatement 在当前Session下创建新的NewStatement
func (b *Session) NewStatement() *Statement {
	return NewStatement(b)
}

//SessionUpdateMsg Batch更新消息
type SessionUpdateMsg struct {
	State string   `json:"State"`
	New   *Session `json:"New"`
	Old   *Session `json:"Old"`
}

func (b *Session) watch(interval time.Duration, ch chan SessionUpdateMsg) {
	defer func() {
		err := recover()
		if err != nil {
			errE := err.(error)
			log.Error(map[string]interface{}{
				"err": errE,
			}, "session watch error")
			if strings.HasPrefix(errE.Error(), "未找到资源") {
				ch <- SessionUpdateMsg{
					State: "cancelled",
				}
			} else {
				ch <- SessionUpdateMsg{
					State: "watch_err",
				}
			}
		}
		close(ch)
	}()
	var oldbb []byte
	var newbb []byte
	bb, err := b.ToJSON()
	if err != nil {
		panic(err)
	}
	oldbb = bb
OuterLoop:
	for {
		oldb := b.Copy()
		gb, err := b.Update()
		if err != nil {
			panic(err)
		}
		if newbb != nil {
			oldbb = newbb
			newbb = gb
		} else {
			newbb = gb
		}

		msg := SessionUpdateMsg{
			State: b.State,
			New:   b,
			Old:   oldb,
		}
		switch b.State {
		case "shutting_down":
			{
				ch <- msg
				break OuterLoop
			}
		case "Error":
			{
				ch <- msg
				break OuterLoop
			}
		case "dead":
			{
				ch <- msg
				break OuterLoop
			}
		case "killed":
			{

				ch <- msg
				break OuterLoop
			}

		case "success":
			{
				ch <- msg
				break OuterLoop
			}
		default:
			{
				if MD5(oldbb) != MD5(newbb) {
					ch <- msg
				}
				time.Sleep(interval)
			}
		}
	}

}

//Watch 轮询监听状态变化
func (b *Session) Watch(interval time.Duration, chanBuffer int) (chan SessionUpdateMsg, error) {
	switch {
	case chanBuffer > 0:
		{
			ch := make(chan SessionUpdateMsg, chanBuffer)
			go b.watch(interval, ch)
			return ch, nil
		}
	case chanBuffer == 0:
		{
			ch := make(chan SessionUpdateMsg)
			go b.watch(interval, ch)
			return ch, nil
		}
	default:
		{
			return nil, errors.New("chanBuffer必须为非负数")
		}
	}
}
