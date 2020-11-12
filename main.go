package main

import (
	"strconv"
	"time"

	log "github.com/Basic-Components/loggerhelper"

	console "github.com/AsynkronIT/goconsole"
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/actor/middleware"
	"github.com/AsynkronIT/protoactor-go/router"
)

type myMessage struct{ i int }

func (m *myMessage) Hash() string {
	return strconv.Itoa(m.i)
}

func main() {
	log.Info(nil, "Round robin routing:")
	system := actor.NewActorSystem()
	rootContext := system.Root
	act := func(context actor.Context) {
		switch msg := context.Message().(type) {
		case *myMessage:
			log.Info(map[string]interface{}{
				"self":    context.Self(),
				"message": msg.i,
			}, "%v got message %d")
		}
	}
	log.Info(nil, "BroadcastGroup routing:")

	pid1 := rootContext.Spawn(actor.PropsFromFunc(act).WithReceiverMiddleware(middleware.Logger))
	pid2 := rootContext.Spawn(actor.PropsFromFunc(act).WithReceiverMiddleware(middleware.Logger))
	pid := rootContext.Spawn(router.NewBroadcastGroup(pid1, pid2).WithReceiverMiddleware(middleware.Logger))
	// for i := 0; i < 3; i++ {
	// 	rootContext.Send(pid, &myMessage{i})
	// }
	rootContext.Send(pid, &myMessage{1})

	pid3 := rootContext.Spawn(actor.PropsFromFunc(act).WithReceiverMiddleware(middleware.Logger))
	rootContext.Send(pid, &router.AddRoutee{
		PID: pid3,
	})
	time.Sleep(1)
	rootContext.Send(pid, &myMessage{9})

	_, _ = console.ReadLine()
}
