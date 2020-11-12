package golivyclient

import (
	"fmt"
	"testing"
	"time"
)

func TestLivyClient_QueryBatch(t *testing.T) {

	Default.Init("http://localhost:8998")
	thirdparty_path := "/user/recommender/recommend/data/tmp/mlflow/lib/thirdparty"
	mlflow_path := "/user/recommender/recommend/data/tmp/mlflow/lib/v1.1"
	sq := NewBatchQuery{
		File:      fmt.Sprintf("%s/mlflow_2.11-1.1.jar", mlflow_path),
		ClassName: "com.xndm.recommend.mlflow.mlflow",
		Args:      []string{"1600849146359"},
		// jars to be used in this session
		Jars: []string{
			fmt.Sprintf("%s/fastjson-1.2.37.jar", thirdparty_path),
			fmt.Sprintf("%s//log4j-core-2.11.2.jar", thirdparty_path),
			fmt.Sprintf("%s//log4j-api-2.13.0.jar", thirdparty_path),
			fmt.Sprintf("%s//gson-2.8.5.jar", thirdparty_path),
			fmt.Sprintf("%s//mysql-connector-java-8.0.18.jar", thirdparty_path),
			fmt.Sprintf("%s//xgboost4j-0.90.jar", thirdparty_path),
			fmt.Sprintf("%s//xgboost4j-spark-0.90.jar", thirdparty_path),
			fmt.Sprintf("%s//akka-actor_2.13-2.5.26.jar", thirdparty_path),
			fmt.Sprintf("%s//jpmml-evaluator-spark-1.2.2.jar", thirdparty_path),
		},
		Files: []string{
			fmt.Sprintf("%s/config/conf.properties", mlflow_path),
			fmt.Sprintf("%s/config/log4j.properties", mlflow_path),
		},
		DriverMemory:   "1G",
		ExecutorMemory: "2G",
		ExecutorCores:  1,
		// The name of the YARN queue to which submitted
		Queue: "recommend",
		// The name of this session
		Name: "mlflow-scala-1232",
		// Spark configuration properties
		Conf: map[string]interface{}{
			"spark.dynamicAllocation.enabled":                         true,
			"spark.shuffle.service.enabled":                           true,
			"spark.dynamicAllocation.maxExecutors":                    12,
			"spark.sql.crossJoin.enabled":                             true,
			"spark.sql.adaptive.enabled":                              true,
			"spark.sql.adaptive.skewedJoin.enabled":                   true,
			"spark.driver.extraJavaOptions":                           "-Dlog4j.configuration=file:log4j.properties",
			"spark.hadoop.ipc.client.fallback-to-simple-auth-allowed": true,
		},
	}
	s := Default.NewBatch()
	s.New(&sq)
	t.Log("get batch info", s.ID)
	ch, err := s.Watch(3*time.Second, 10)
	if err != nil {
		t.Error(err)
	}
	for i := range ch {
		t.Log("get info", i)
	}
	t.Log("get info done")
}

// func TestLivyClient_QuerySession(t *testing.T) {

// 	Default.Init("http://localhost:8998")
// 	sq := NewSessionQuery{
// 		Kind:                     "spark",
// 		HeartbeatTimeoutInSecond: 60 * 10,
// 	}
// 	s := Default.NewSession()
// 	s.New(&sq)
// 	defer s.Close()
// 	t.Log("get session info", s.ID)
// 	ss := s.NewStatement()
// 	ssq := NewStatementQuery{
// 		Code: `val NUM_SAMPLES = 100000;
// 		val count = sc.parallelize(1 to NUM_SAMPLES).map { i =>
// 		  val x = Math.random();
// 		  val y = Math.random();
// 		  if (x*x + y*y < 1) 1 else 0
// 		}.reduce(_ + _);
// 		println(\"Pi is roughly \" + 4.0 * count / NUM_SAMPLES)
// 		`,
// 	}
// 	ss.New(&ssq)
// 	ch, err := ss.Watch(3*time.Second, 10)
// 	if err != nil {
// 		t.Error(err)
// 	}
// 	for i := range ch {
// 		t.Log("get info", i)
// 	}
// 	t.Log("get info done")
// }
