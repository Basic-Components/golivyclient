package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/actor/middleware"
	log "github.com/Basic-Components/loggerhelper"
	"github.com/robfig/cron/v3"
)

//Batch livy的批,用于管理固定任务
type Batch struct {
	Client    *LivyClient            `json:"-"`
	URI       string                 `json:"-"`
	Pid       *actor.PID             `json:"-"`
	c         *cron.Cron             `json:"-"`
	Query     *NewBatchQuery         `json:"query"`
	CrotabStr string                 `json:"crontab"`
	ID        int                    `json:"id"`
	AppID     string                 `json:"appId"`
	AppInfo   map[string]interface{} `json:"appInfo"`
	Log       []string               `json:"log"`
	State     string                 `json:"state"`
}

//NewBatchQuery livy批的创建请求,用于提交固定任务
type NewBatchQuery struct {
	// File containing the application to execute
	File string `json:"file"`
	// User to impersonate when starting the batch
	ProxyUser string `json:"proxyUser,omitempty"`
	// Application Java/Spark main class
	ClassName string `json:"className,omitempty"`
	// Command line arguments for the application
	Args []string `json:"args,omitempty"`
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
	// The name of this session
	Name string `json:"name,omitempty"`
	// Spark configuration properties
	Conf map[string]interface{} `json:"conf,omitempty"`
}

//NewBatch 创建新的Batch
func NewBatch(c *LivyClient, crotabStr string, query *NewBatchQuery) *Batch {
	b := new(Batch)
	uri := "batches"
	b.Client = c
	b.URI = uri
	b.Query = query
	b.CrotabStr = crotabStr
	b.Pid = b.Client.rootContext.Spawn(actor.PropsFromProducer(func() actor.Actor { return b }).WithReceiverMiddleware(middleware.Logger))
	return b
}

//Receive 满足Actor接口
func (b *Batch) Receive(context actor.Context) {
	switch msg := context.Message().(type) {
	case *actor.Started:
		{
			log.Info(nil, "batch query start")
			err := b.new()
			if err != nil {
				b.State = "error"
				b.Close()
			}
			b.c = cron.New()
			b.c.AddFunc(
				fmt.Sprintf("@every %s", b.CrotabStr),
				func() {
					fmt.Println("Every hour thirty, starting an hour thirty from now")
				})
			b.c.Start()
		}
	case *actor.Stopping:
		{
			log.Info(nil, "Stopping, actor is about shut down")
			if b.c != nil {
				b.c.Stop()
			}
		}

	case *actor.Stopped:
		log.Info(nil, "Stopped, actor and its children are stopped")
	case *actor.Restarting:
		log.Info(nil, "Restarting, actor is about restart")
	case []byte:
		fmt.Printf("Hello %v\n", msg.Who)
		panic("Ouch")
	}
}

//new 新建一个batch请求并将结果更新到自身
func (b *Batch) new() error {
	url := fmt.Sprintf("%s/%s", b.Client.BASEURL, b.URI)
	resBytes, err := HTTPJSONQuery(url, "POST", b.Query)
	if err != nil {
		return err
	}
	json.Unmarshal(resBytes, b)
	return nil
}

//Close 关闭actor
func (b *Batch) Close() {
	b.Client.rootContext.Stop(b.Pid)
}

//Copy 克隆一份当前的状态
func (b *Batch) Copy() *Batch {

	newAppInfo := map[string]interface{}{}
	for key, value := range b.AppInfo {
		newAppInfo[key] = value
	}
	newlog := []string{}
	for _, ele := range b.Log {
		newlog = append(newlog, ele)
	}

	newone := Batch{
		Client:  b.Client,
		URI:     b.URI,
		ID:      b.ID,
		AppID:   b.AppID,
		AppInfo: newAppInfo,
		Log:     newlog,
		State:   b.State,
	}
	return &newone
}

//ToJSONString 将消息转未json字符串
func (b *Batch) ToJSONString() (string, error) {
	return json.MarshalToString(b)
}

//ToJSON 将消息转未json
func (b *Batch) ToJSON() ([]byte, error) {
	return json.Marshal(b)
}

//queryBytesInfo 请求接口获取Batch对象的信息的字节串
func (b *Batch) queryBytesInfo() ([]byte, error) {
	url := fmt.Sprintf("%s/%s/%d", b.Client.BASEURL, b.URI, b.ID)
	resBytes, err := HTTPJSONQuery(url, "GET")
	if err != nil {
		return nil, err
	}
	return resBytes, nil
}

// //queryInfo 请求接口获取Batch对象的信息
// func (b *Batch) queryInfo() (*Batch, error) {
// 	resb, err := b.queryBytesInfo()
// 	if err != nil {
// 		return nil, err
// 	}
// 	nb := Batch{}
// 	err = json.Unmarshal(resb, &nb)
// 	if err != nil {
// 		return nil, err
// 	}
// 	nb.Client = b.Client
// 	nb.URI = b.URI
// 	return &nb, nil
// }

//update 更新自身
func (b *Batch) updateSelf() ([]byte, error) {
	resb, err := b.queryBytesInfo()
	if err != nil {
		return nil, err
	}
	nb := Batch{}
	err = json.Unmarshal(resb, &nb)
	if err != nil {
		return nil, err
	}
	b.ID = nb.ID
	b.AppID = nb.AppID
	b.AppInfo = nb.AppInfo
	b.Log = nb.Log
	b.State = nb.State
	return resb, nil
}

//Kill 关闭batch所指向的任务
func (b *Batch) Kill() error {
	url := fmt.Sprintf("%s/%s/%d", b.Client.BASEURL, b.URI, b.ID)
	_, err := HTTPJSONQuery(url, "DELETE")
	if err != nil {
		return err
	}
	return nil
}

//BatchUpdateMsg Batch更新消息
type BatchUpdateMsg struct {
	State string `json:"State"`
	New   *Batch `json:"New"`
	Old   *Batch `json:"Old"`
}

func (b *Batch) watch(interval time.Duration, ch chan BatchUpdateMsg) {
	defer func() {
		err := recover()
		if err != nil {
			errE := err.(error)
			log.Error(map[string]interface{}{
				"err": errE,
			}, "batch watch error")
			if strings.HasPrefix(errE.Error(), "未找到资源") {
				ch <- BatchUpdateMsg{
					State: "cancelled",
				}
			} else {
				ch <- BatchUpdateMsg{
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

		msg := BatchUpdateMsg{
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
		case "error":
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
//@interval time.Duration 轮询间隔时间
//@chanBuffer int 队列长度
func (b *Batch) Watch(interval time.Duration, chanBuffer int) (chan BatchUpdateMsg, error) {
	switch {
	case chanBuffer > 0:
		{
			ch := make(chan BatchUpdateMsg, chanBuffer)
			go b.watch(interval, ch)
			return ch, nil
		}
	case chanBuffer == 0:
		{
			ch := make(chan BatchUpdateMsg)
			go b.watch(interval, ch)
			return ch, nil
		}
	default:
		{
			return nil, errors.New("chanBuffer必须为非负数")
		}
	}
}
