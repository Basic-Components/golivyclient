package golivyclient

import (
	"testing"
	"time"
)

func TestLivyClient_QuerySession(t *testing.T) {

	Default.Init("http://localhost:8998")
	sq := NewSessionQuery{
		Kind:                     "spark",
		HeartbeatTimeoutInSecond: 60 * 10,
	}
	s := Default.NewSession()
	s.New(&sq)
	defer s.Close()
	t.Log("get session info", s.ID)
	ss := s.NewStatement()
	ssq := NewStatementQuery{
		Code: `val NUM_SAMPLES = 100000;
		val count = sc.parallelize(1 to NUM_SAMPLES).map { i =>
		  val x = Math.random();
		  val y = Math.random();
		  if (x*x + y*y < 1) 1 else 0
		}.reduce(_ + _);
		println(\"Pi is roughly \" + 4.0 * count / NUM_SAMPLES)
		`,
	}
	ss.New(&ssq)
	ch, err := ss.Watch(3*time.Second, 10)
	if err != nil {
		t.Error(err)
	}
	for i := range ch {
		t.Log("get info", i)
	}
	t.Log("get info done")
}
