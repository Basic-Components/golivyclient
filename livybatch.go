package main

import (
	"fmt"

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
	listeners []chan Event           `json:"-"`
	Query     *NewBatchQuery         `json:"-"`
	CrotabStr string                 `json:"-"`
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
					// fmt.Println("Every hour thirty, starting an hour thirty from now")
					err := b.updateSelf()
					if err != nil {
						log.Error(map[string]interface{}{
							"err": err,
						}, "query get error")
					}
					// } else {

					// 	b.Client.rootContext.Send(b.Pid, Event{
					// 		Name:        "UPDATE",
					// 		MessageJSON: msg,
					// 	})
					// }
				})
			b.c.Start()
		}
	case *actor.Stopping:
		{
			b.c.Stop()
			fmt.Println("Stopping, actor is about shut down")
		}

	case PutEvent:
		{

		}
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
	b.c.Stop()
	b.Client.rootContext.Stop(b.Pid)
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

//update 更新自身
func (b *Batch) updateSelf() error {
	messageJSON, err := b.queryBytesInfo()
	if err != nil {
		return err
	}
	lenoldlog := len(b.Log)
	oldstate := b.State
	err = json.Unmarshal(messageJSON, &b)
	if err != nil {
		return err
	}
	if b.State != oldstate || len(b.Log) != lenoldlog {
		bjson, err := b.ToJSONString()
		if err != nil {
			return err
		}
		b.Client.rootContext.Send(
			b.Pid,
			PutEvent{
				Message: bjson,
			},
		)
	}
	switch b.State {
	case "shutting_down":
		{
			b.Client.rootContext.Send(
				b.Pid,
				DeleteEvent{},
			)

		}
	case "error":
		{
			b.Client.rootContext.Send(
				b.Pid,
				DeleteEvent{},
			)

		}
	case "dead":
		{
			b.Client.rootContext.Send(
				b.Pid,
				DeleteEvent{},
			)

		}
	case "killed":
		{

			b.Client.rootContext.Send(
				b.Pid,
				DeleteEvent{},
			)

		}

	case "success":
		{
			b.Client.rootContext.Send(
				b.Pid,
				DeleteEvent{},
			)

		}
	}
	return nil
}

//Cancel 取消任务
func (b *Batch) Cancel() error {
	url := fmt.Sprintf("%s/%s/%d", b.Client.BASEURL, b.URI, b.ID)
	_, err := HTTPJSONQuery(url, "DELETE")
	if err != nil {
		return err
	}
	b.State = "cancelled"
	b.Close()
	return nil
}

//Watch 轮询监听状态变化
func (b *Session) Watch(chan Event) {

}
