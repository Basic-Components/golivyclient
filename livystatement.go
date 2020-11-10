package golivyclient

import (
	jsonl "encoding/json"
	"errors"
	"fmt"
	"time"

	log "github.com/Basic-Components/loggerhelper"
)

//StatementOutput  会话请求的输出结果
type StatementOutput struct {
	Status         string            `json:"status"`
	ExecutionCount int               `json:"execution_count"`
	Data           *jsonl.RawMessage `json:"data"`
}

//Statement livy的会话的请求,用于管理交互模式提交的代码
type Statement struct {
	Session   *Session         `json:"-"`
	URI       string           `json:"-"`
	ID        int              `json:"id"`
	Code      string           `json:"code"`
	Output    *StatementOutput `json:"output"`
	Progress  float64          `json:"progress"`
	Started   int64            `json:"started"`
	Completed int64            `json:"completed"`
	State     string           `json:"state"`
}

//NewStatementQuery livy的创建请求,用于提交固定任务
type NewStatementQuery struct {
	Code   string `json:"code,omitempty"`
	Kind   string `json:"kind,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

//NewStatement 创建Session中新的Statement
func NewStatement(s *Session) *Statement {
	b := new(Statement)
	uri := "statements"
	b.Session = s
	b.URI = uri
	b.Session.Statements = append(b.Session.Statements, b)
	return b
}

//New 创建Statement种新的Statement的请求,结果更新到自身
func (b *Statement) New(q *NewStatementQuery) error {

	url := fmt.Sprintf("%s/%s/%d/%s", b.Session.Client.BASEURL, b.Session.URI, b.Session.ID, b.URI)
	resBytes, err := HTTPJSONQuery(url, "POST", q)
	if err != nil {
		return err
	}
	json.Unmarshal(resBytes, b)
	return nil
}

//Copy 克隆一份当前的状态
func (b *Statement) Copy() *Statement {

	newone := Statement{
		Session:   b.Session,
		URI:       b.URI,
		ID:        b.ID,
		Code:      b.Code,
		Output:    b.Output,
		Progress:  b.Progress,
		Started:   b.Started,
		Completed: b.Completed,
		State:     b.State,
	}
	return &newone
}

//ToJSONString 将消息转未json字符串
func (b *Statement) ToJSONString() (string, error) {
	return json.MarshalToString(b)
}

//ToJSON 将消息转未json
func (b *Statement) ToJSON() ([]byte, error) {
	return json.Marshal(b)
}

//BytesInfo 获取Statement对象的信息
func (b *Statement) BytesInfo() ([]byte, error) {
	url := fmt.Sprintf("%s/%s/%d/%s/%d", b.Session.Client.BASEURL, b.Session.URI, b.Session.ID, b.URI, b.ID)
	resBytes, err := HTTPJSONQuery(url, "GET")
	if err != nil {
		return nil, err
	}
	return resBytes, nil
}

//Info 获取Statement对象的信息
func (b *Statement) Info() (*Statement, error) {
	resb, err := b.BytesInfo()
	if err != nil {
		return nil, err
	}
	nb := Statement{}
	err = json.Unmarshal(resb, &nb)
	if err != nil {
		return nil, err
	}
	nb.Session = b.Session
	nb.URI = b.URI
	return &nb, nil
}

//Update 更新自身
func (b *Statement) Update() ([]byte, error) {
	resb, err := b.BytesInfo()
	if err != nil {
		return nil, err
	}
	nb := Statement{}
	err = json.Unmarshal(resb, &nb)
	if err != nil {
		return nil, err
	}
	b.ID = nb.ID
	b.Code = nb.Code
	b.Output = nb.Output
	b.Progress = nb.Progress
	b.Started = nb.Started
	b.Completed = nb.Completed
	b.State = nb.State
	return resb, nil
}

//Cancel 取消代码执行
func (b *Statement) Cancel() error {
	url := fmt.Sprintf("%s/%s/%d/%s/%d/cancel", b.Session.Client.BASEURL, b.Session.URI, b.Session.ID, b.URI, b.ID)
	_, err := HTTPJSONQuery(url, "POST")
	if err != nil {
		return err
	}
	return nil
}

//StatementUpdateMsg Batch更新消息
type StatementUpdateMsg struct {
	State string
	New   *Statement
	Old   *Statement
}

func (b *Statement) watch(interval time.Duration, ch chan StatementUpdateMsg) {
	defer func() {
		err := recover()
		if err != nil {
			log.Error(map[string]interface{}{
				"err": err,
			}, "batch watch error")
			ch <- StatementUpdateMsg{
				State: "watch_err",
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
		msg := StatementUpdateMsg{
			State: b.State,
			New:   b,
			Old:   oldb,
		}
		switch b.State {
		case "cancelled":
			{
				ch <- msg
				break OuterLoop
			}
		case "Error":
			{
				ch <- msg
				break OuterLoop
			}
		case "available":
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
func (b *Statement) Watch(interval time.Duration, chanBuffer int) (chan StatementUpdateMsg, error) {
	switch {
	case chanBuffer > 0:
		{
			ch := make(chan StatementUpdateMsg, chanBuffer)
			go b.watch(interval, ch)
			return ch, nil
		}
	case chanBuffer == 0:
		{
			ch := make(chan StatementUpdateMsg)
			go b.watch(interval, ch)
			return ch, nil
		}
	default:
		{
			return nil, errors.New("chanBuffer必须为非负数")
		}
	}
}
