package report

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	apmmodel "github.com/CloudDetail/apo-module/apm/model/v1"
	"github.com/CloudDetail/apo-module/model/v1"
	"github.com/CloudDetail/apo-receiver/pkg/analyzer/external"
)

func TestOtelV1Relation(t *testing.T) {
	testRelations(t, "otel-1.32.0", map[string][]*Relationship{
		"http": {
			newRelationship("093fa92dd5642ee6_0.").
				withService("stuck-demo-tomcat", "GET /wait/callOthers"),
			newRelationship("093fa92dd5642ee6_0.0.").
				withParent("stuck-demo-tomcat", "GET /wait/callOthers").
				withExternalClient("http", "localhost:9999", "GET /cpu").
				withService("stuck-demo-undertow", "GET /cpu/loop/{times}"),
		},
		"mysql": {
			newRelationship("e71d73f777cfd371_0.").
				withService("stuck-demo-tomcat", "GET /db/query"),
			newRelationship("e71d73f777cfd371_0.0.").
				withParent("stuck-demo-tomcat", "GET /db/query").
				withDbClient("mysql", "localhost:3306", "SELECT test.weather"),
			newRelationship("e71d73f777cfd371_0.1.").
				withParent("stuck-demo-tomcat", "GET /db/query").
				withDbClient("mysql", "localhost:3306", "SELECT test.weather"),
		},
		"redis": {
			newRelationship("4f6fa1492b50ef13_0.").
				withService("stuck-demo-tomcat", "GET /redis/query"),
			newRelationship("4f6fa1492b50ef13_0.0.").
				withParent("stuck-demo-tomcat", "GET /redis/query").
				withDbClient("redis", "localhost:6379", "SET"),
			newRelationship("4f6fa1492b50ef13_0.1.").
				withParent("stuck-demo-tomcat", "GET /redis/query").
				withDbClient("redis", "localhost:6379", "EXISTS"),
		},
		"dubbo": {
			newRelationship("1b2a511ffcb6ad86_0.").
				withService("dubbo-consumer", "GET /dubbo/{sleepA}/{sleepB}/{sleepC}"),
			newRelationship("1b2a511ffcb6ad86_0.0.").
				withParent("dubbo-consumer", "GET /dubbo/{sleepA}/{sleepB}/{sleepC}").
				withExternalClient("apache_dubbo", "localhost:30002", "io.apo.dubbo.api.service.OrderService/order2").
				withService("dubbo-provider", "io.apo.dubbo.api.service.OrderService/order2"),
		},
		"grpc": {
			newRelationship("1c298dd6fda802e0_0.").
				withService("grpc-client", "POST /grpc"),
			newRelationship("1c298dd6fda802e0_0.0.").
				withParent("grpc-client", "POST /grpc").
				withExternalClient("grpc", "localhost:9002", "Greeter/SayHello").
				withService("grpc-server", "Greeter/SayHello"),
		},
		"activemq": {
			newRelationship("16cfe3650b5edc18_0.").
				withService("activemq-provider", "GET /send"),
			newRelationship("16cfe3650b5edc18_0.0.").
				withParent("activemq-provider", "GET /send").
				withMqClient("jms", "", "ActiveMQQueue").
				withService("activemq-consumer", "ActiveMQQueue process").
				withAsync(),
		},
		"rabbitmq": {
			newRelationship("324b786fe2671286_0.").
				withService("rabbitmq-provider", "GET /send"),
			newRelationship("324b786fe2671286_0.2.").
				withParent("rabbitmq-provider", "GET /send").
				withMqClient("rabbitmq", "localhost:5672", "exchange.declare"),
			newRelationship("324b786fe2671286_0.3.").
				withParent("rabbitmq-provider", "GET /send").
				withMqClient("rabbitmq", "localhost:5672", "exchange.declare"),
			newRelationship("324b786fe2671286_0.4.").
				withParent("rabbitmq-provider", "GET /send").
				withMqClient("rabbitmq", "localhost:5672", "queue.declare"),
			newRelationship("324b786fe2671286_0.5.").
				withParent("rabbitmq-provider", "GET /send").
				withMqClient("rabbitmq", "localhost:5672", "queue.bind"),
			newRelationship("324b786fe2671286_0.0.").
				withParent("rabbitmq-provider", "GET /send").
				withMqClient("rabbitmq", "", "TestDirectExchange").
				withService("rabbitmq-consumer", "TestDirectQueue process").
				withAsync(),
			newRelationship("324b786fe2671286_0.1.").
				withParent("rabbitmq-provider", "GET /send").
				withMqClient("rabbitmq", "", "TestDirectRouting").
				withService("rabbitmq-consumer", "TestDirectRouting process").
				withAsync(),
		},
		"rocketmq": {
			newRelationship("67d44c72f125ac62_0.").
				withService("rocketmq-provider", "GET /send"),
			newRelationship("67d44c72f125ac62_0.0.").
				withParent("rocketmq-provider", "GET /send").
				withMqClient("rocketmq", "", "cart-item-add-topic").
				withService("rocketmq-consumer", "cart-item-add-topic process").
				withAsync(),
		},
		"kafka": {
			newRelationship("be7dd55ce5119c62_0.").
				withService("kafka-provider", "GET /send"),
			newRelationship("be7dd55ce5119c62_0.0.").
				withParent("kafka-provider", "GET /send").
				withMqClient("kafka", "", "topic_login").
				withService("kafka-consumer", "topic_login process").
				withAsync(),
		},
	})
}

func TestOteV2Relation(t *testing.T) {
	testRelations(t, "otel-2.9.0", map[string][]*Relationship{
		"http": {
			newRelationship("e839fc54b8e0b748_0.").
				withService("stuck-tomcat", "GET /wait/callOthers"),
			newRelationship("e839fc54b8e0b748_0.0.").
				withParent("stuck-tomcat", "GET /wait/callOthers").
				withExternalClient("http", "localhost:9999", "GET /cpu").
				withService("stuck-undertow", "GET /cpu/loop/{times}"),
		},
		"mysql": {
			newRelationship("91ed719bee9e4b3d_0.").
				withService("stuck-demo-tomcat", "GET /db/query"),
			newRelationship("91ed719bee9e4b3d_0.0.").
				withParent("stuck-demo-tomcat", "GET /db/query").
				withDbClient("mysql", "localhost:3306", "SELECT test.weather"),
			newRelationship("91ed719bee9e4b3d_0.1.").
				withParent("stuck-demo-tomcat", "GET /db/query").
				withDbClient("mysql", "localhost:3306", "SELECT test.weather"),
		},
		"redis": {
			newRelationship("a0a95ae3da065bce_0.").
				withService("stuck-demo-tomcat", "GET /redis/query"),
			newRelationship("a0a95ae3da065bce_0.0.").
				withParent("stuck-demo-tomcat", "GET /redis/query").
				withDbClient("redis", "localhost:6379", "SET"),
			newRelationship("a0a95ae3da065bce_0.1.").
				withParent("stuck-demo-tomcat", "GET /redis/query").
				withDbClient("redis", "localhost:6379", "EXISTS"),
		},
		"dubbo": {
			newRelationship("f9e7d9e10db59547_0.").
				withService("dubbo-consumer", "GET /dubbo/{sleepA}/{sleepB}/{sleepC}"),
			newRelationship("f9e7d9e10db59547_0.0.").
				withParent("dubbo-consumer", "GET /dubbo/{sleepA}/{sleepB}/{sleepC}").
				withService("dubbo-provider", "io.apo.dubbo.api.service.OrderService/order2").
				withExternalClient("apache_dubbo", "localhost:30002", "io.apo.dubbo.api.service.OrderService/order2"),
		},
		"grpc": {
			newRelationship("47df06483d41e41c_0.").
				withService("grpc-client", "GET /test"),
			newRelationship("47df06483d41e41c_0.0.").
				withParent("grpc-client", "GET /test").
				withExternalClient("grpc", "localhost:9002", "Greeter/SayHello").
				withService("grpc-server", "Greeter/SayHello"),
		},
		"activemq": {
			newRelationship("2b773c7586c94fa2_0.").
				withService("activemq-provider", "GET /send"),
			newRelationship("2b773c7586c94fa2_0.0.").
				withParent("activemq-provider", "GET /send").
				withMqClient("jms", "", "ActiveMQQueue").
				withService("activemq-consumer", "ActiveMQQueue process").
				withAsync(),
		},
		"rabbitmq": {
			newRelationship("dc6a7e596a5049ac_0.").
				withService("rabbitmq-provider", "GET /send"),
			newRelationship("dc6a7e596a5049ac_0.2.").
				withParent("rabbitmq-provider", "GET /send").
				withMqClient("rabbitmq", "localhost:5672", "exchange.declare"),
			newRelationship("dc6a7e596a5049ac_0.3.").
				withParent("rabbitmq-provider", "GET /send").
				withMqClient("rabbitmq", "localhost:5672", "queue.declare"),
			newRelationship("dc6a7e596a5049ac_0.4.").
				withParent("rabbitmq-provider", "GET /send").
				withMqClient("rabbitmq", "localhost:5672", "queue.bind"),
			newRelationship("dc6a7e596a5049ac_0.5.").
				withParent("rabbitmq-provider", "GET /send").
				withMqClient("rabbitmq", "localhost:5672", "exchange.declare"),
			newRelationship("dc6a7e596a5049ac_0.0.").
				withParent("rabbitmq-provider", "GET /send").
				withMqClient("rabbitmq", "", "TestDirectRouting").
				withService("rabbitmq-consumer", "TestDirectRouting process").
				withAsync(),
			newRelationship("dc6a7e596a5049ac_0.1.").
				withParent("rabbitmq-provider", "GET /send").
				withMqClient("rabbitmq", "localhost:5672", "TestDirectExchange").
				withService("rabbitmq-consumer", "TestDirectQueue process").
				withAsync(),
		},
		"rocketmq": {
			newRelationship("4da0b3ecd83e0ea7_0.").
				withService("rocketmq-provider", "GET /send"),
			newRelationship("4da0b3ecd83e0ea7_0.0.").
				withParent("rocketmq-provider", "GET /send").
				withService("rocketmq-consumer", "cart-item-add-topic process").
				withMqClient("rocketmq", "", "cart-item-add-topic").
				withAsync(),
		},
		"kafka": {
			newRelationship("791ffe785bab9958_0.").
				withService("kafka-provider", "GET /send"),
			newRelationship("791ffe785bab9958_0.0.").
				withParent("kafka-provider", "GET /send").
				withMqClient("kafka", "", "topic_login").
				withService("kafka-consumer", "topic_login process").
				withAsync(),
		},
	})
}

func TestSkywalkingRelation(t *testing.T) {
	testRelations(t, "skywalking", map[string][]*Relationship{
		"http": {
			newRelationship("3f965182dba01b2b_0.").
				withService("stuck-demo", "GET:/callOthers"),
			newRelationship("3f965182dba01b2b_0.0.").
				withParent("stuck-demo", "GET:/callOthers").
				withExternalClient("http", "localhost:9999", "GET /db").
				withService("stuck-demo-undertow", "GET:/db/query"),
			newRelationship("3f965182dba01b2b_0.0.0.").
				withParent("stuck-demo-undertow", "GET:/db/query").
				withDbClient("mysql", "localhost:3306", "SELECT test.weather"),
			newRelationship("3f965182dba01b2b_0.0.1.").
				withParent("stuck-demo-undertow", "GET:/db/query").
				withDbClient("mysql", "localhost:3306", "SELECT test.weather"),
		},
		"error": {
			newRelationship("65e57d30dcfb44d1_0.").
				withService("stuck-demo", "GET:/wait/callOthers"),
			newRelationship("65e57d30dcfb44d1_0.0.").
				withParent("stuck-demo", "GET:/wait/callOthers").
				withExternalClient("http", "localhost:9999", "GET /wait").
				withService("stuck-demo-undertow", "GET:/wait/fail"),
		},
		"redis": {
			newRelationship("8117319bec6eda42_0.").
				withService("stuck-demo-tomcat", "GET:/redis/query"),
			newRelationship("8117319bec6eda42_0.0.").
				withParent("stuck-demo-tomcat", "GET:/redis/query").
				withDbClient("redis", "localhost:6379", "Lettuce/EXISTS"),
			newRelationship("8117319bec6eda42_0.1.").
				withParent("stuck-demo-tomcat", "GET:/redis/query").
				withDbClient("redis", "localhost:6379", "Lettuce/SET"),
		},
		"dubbo": {
			newRelationship("98d26201482ece3e_0.").
				withService("dubbo-consumer", "GET:/dubbo/{sleepA}/{sleepB}/{sleepC}"),
			newRelationship("98d26201482ece3e_0.0.").
				withParent("dubbo-consumer", "GET:/dubbo/{sleepA}/{sleepB}/{sleepC}").
				withExternalClient("dubbo", "localhost:30002", "io.apo.dubbo.api.service.OrderService.order2(long,long)").
				withService("dubbo-provider", "io.apo.dubbo.api.service.OrderService.order2(long,long)"),
		},
		"grpc": {
			newRelationship("c5d652e60b91ad8c_0.").
				withService("grpc-client", "POST:/grpc"),
			newRelationship("c5d652e60b91ad8c_0.0.").
				withParent("grpc-client", "POST:/grpc").
				withExternalClient("grpc", "springboot-grpc-server", "Greeter.sayHello").
				withService("grpc-server", "Greeter.sayHello"),
		},
		"activemq": {
			newRelationship("fb9245dd1fef6738_0.").
				withService("activemq-provider", "GET:/send"),
			newRelationship("fb9245dd1fef6738_0.0.").
				withParent("activemq-provider", "GET:/send").
				withMqClient("activemq", "localhost:61616", "ActiveMQQueue").
				withService("activemq-consumer", "ActiveMQ/Queue/ActiveMQQueue/Consumer").
				withAsync(),
		},
		"rabbitmq": {
			newRelationship("ff775e9480c9d861_0.").
				withService("rabbitmq-provider", "GET:/send"),
			newRelationship("ff775e9480c9d861_0.0.").
				withParent("rabbitmq-provider", "GET:/send").
				withMqClient("rabbitmq", "localhost:5672", "TestDirectRouting").
				withService("rabbitmq-consumer", "RabbitMQ/Topic/TestDirectExchangeQueue/TestDirectRouting/Consumer").
				withAsync(),
		},
		"rocketmq": {
			newRelationship("674569984ff43c17_0.").
				withService("rocketmq-provider", "GET:/send"),
			newRelationship("674569984ff43c17_0.0.").
				withParent("rocketmq-provider", "GET:/send").
				withMqClient("rocketmq", "localhost:10911", "cart-item-add-topic").
				withService("rocketmq-consumer", "RocketMQ/cart-item-add-topic/Consumer").
				withAsync(),
		},
		"kafka": {
			newRelationship("dcded06df9ae8e79_0.").
				withService("kafka-provider", "GET:/send"),
			newRelationship("dcded06df9ae8e79_0.0.").
				withParent("kafka-provider", "GET:/send").
				withMqClient("kafka", "localhost:9092", "topic_login").
				withService("kafka-consumer", "Kafka/topic_login/Consumer/group1").
				withAsync(),
		},
	})
}

func TestUnknownRelation(t *testing.T) {
	testRelations(t, "unknown", map[string][]*Relationship{
		"http": {
			newRelationship("093fa92dd5642ee6_0.").
				withService("stuck-demo-tomcat", "GET /wait/callOthers"),
			newRelationship("093fa92dd5642ee6_0.0.").
				withParent("stuck-demo-tomcat", "GET /wait/callOthers").
				withExternalClient("unknown", "", "GET").
				withService("stuck-demo-undertow", "GET /cpu/loop/{times}"),
		},
		"mysql": {
			newRelationship("e71d73f777cfd371_0.").
				withService("stuck-demo-tomcat", "GET /db/query"),
			newRelationship("e71d73f777cfd371_0.0.").
				withParent("stuck-demo-tomcat", "GET /db/query").
				withExternalClient("unknown", "", "SELECT test.weather"),
		},
		"redis": {
			newRelationship("4f6fa1492b50ef13_0.").
				withService("stuck-demo-tomcat", "GET /redis/query"),
			newRelationship("4f6fa1492b50ef13_0.0.").
				withParent("stuck-demo-tomcat", "GET /redis/query").
				withExternalClient("unknown", "", "SET"),
		},
		"dubbo": {
			newRelationship("1b2a511ffcb6ad86_0.").
				withService("dubbo-consumer", "GET /dubbo/{sleepA}/{sleepB}/{sleepC}"),
			newRelationship("1b2a511ffcb6ad86_0.0.").
				withParent("dubbo-consumer", "GET /dubbo/{sleepA}/{sleepB}/{sleepC}").
				withExternalClient("unknown", "", "io.apo.dubbo.api.service.OrderService/order2").
				withService("dubbo-provider", "io.apo.dubbo.api.service.OrderService/order2"),
		},
		"grpc": {
			newRelationship("1c298dd6fda802e0_0.").
				withService("grpc-client", "POST /grpc"),
			newRelationship("1c298dd6fda802e0_0.0.").
				withParent("grpc-client", "POST /grpc").
				withExternalClient("unknown", "springboot-grpc-server", "Greeter/SayHello").
				withService("grpc-server", "Greeter/SayHello"),
		},
		"activemq": {
			newRelationship("16cfe3650b5edc18_0.").
				withService("activemq-provider", "GET /send"),
			newRelationship("16cfe3650b5edc18_0.0.").
				withParent("activemq-provider", "GET /send").
				withMqClient("unknown", "", "unknown").
				withService("activemq-consumer", "ActiveMQQueue process").
				withAsync(),
		},
		"rabbitmq": {
			newRelationship("324b786fe2671286_0.").
				withService("rabbitmq-provider", "GET /send"),
			newRelationship("324b786fe2671286_0.2.").
				withParent("rabbitmq-provider", "GET /send").
				withExternalClient("unknown", "", "exchange.declare"),
			newRelationship("324b786fe2671286_0.3.").
				withParent("rabbitmq-provider", "GET /send").
				withExternalClient("unknown", "", "exchange.declare"),
			newRelationship("324b786fe2671286_0.4.").
				withParent("rabbitmq-provider", "GET /send").
				withExternalClient("unknown", "", "queue.declare"),
			newRelationship("324b786fe2671286_0.5.").
				withParent("rabbitmq-provider", "GET /send").
				withExternalClient("unknown", "", "queue.bind"),
			newRelationship("324b786fe2671286_0.0.").
				withParent("rabbitmq-provider", "GET /send").
				withMqClient("unknown", "", "unknown").
				withService("rabbitmq-consumer", "TestDirectQueue process").
				withAsync(),
			newRelationship("324b786fe2671286_0.1.").
				withParent("rabbitmq-provider", "GET /send").
				withMqClient("unknown", "", "unknown").
				withService("rabbitmq-consumer", "TestDirectRouting process").
				withAsync(),
		},
		"rocketmq": {
			newRelationship("67d44c72f125ac62_0.").
				withService("rocketmq-provider", "GET /send"),
			newRelationship("67d44c72f125ac62_0.0.").
				withParent("rocketmq-provider", "GET /send").
				withMqClient("unknown", "", "unknown").
				withService("rocketmq-consumer", "cart-item-add-topic process").
				withAsync(),
		},
		"kafka": {
			newRelationship("be7dd55ce5119c62_0.").
				withService("kafka-provider", "GET /send"),
			newRelationship("be7dd55ce5119c62_0.0.").
				withParent("kafka-provider", "GET /send").
				withMqClient("unknown", "", "unknown").
				withService("kafka-consumer", "topic_login process").
				withAsync(),
		},
	})
}

func testRelations(t *testing.T, apmType string, data map[string][]*Relationship) {
	for testCase, expects := range data {
		testClientCase := buildRelationDatas(t, apmType, testCase)
		if testClientCase != nil {
			checkIntEqual(t, testCase, "Datas Size", len(expects), len(testClientCase))
			for i, expect := range expects {
				got := testClientCase[i]

				checkStringEqual(t, testCase, i, "path", expect.Path, got.Path)
				checkStringEqual(t, testCase, i, "parentService", expect.ParentService, got.ParentService)
				checkStringEqual(t, testCase, i, "parentUrl", expect.ParentUrl, got.ParentUrl)
				checkBoolEqual(t, testCase, i, "parentTraced", expect.ParentTraced, got.ParentTraced)
				checkStringEqual(t, testCase, i, "service", expect.Service, got.Service)
				checkStringEqual(t, testCase, i, "url", expect.Url, got.Url)
				checkStringEqual(t, testCase, i, "clientGroup", expect.ClientGroup, got.ClientGroup)
				checkStringEqual(t, testCase, i, "clientType", expect.ClientType, got.ClientType)
				checkStringEqual(t, testCase, i, "clientPeer", expect.ClientPeer, got.ClientPeer)
				checkStringEqual(t, testCase, i, "clientKey", expect.ClientKey, got.ClientKey)
				checkBoolEqual(t, testCase, i, "isTraced", expect.IsTraced, got.IsTraced)
				checkBoolEqual(t, testCase, i, "isAsync", expect.IsAsync, got.IsAsync)
			}
		}
	}
}

func buildRelationDatas(t *testing.T, apmType string, testCase string) []*Relationship {
	dataFile := fmt.Sprintf("../external/testdata/%s/%s.json", apmType, testCase)
	if !fileExist(dataFile) {
		t.Errorf("TestCase %s-%s is not exist\n", apmType, testCase)
		return nil
	}
	data, err := os.ReadFile(dataFile)
	if err != nil {
		t.Errorf("Fail to read testCase: %s-%s, Error: %v", apmType, testCase, err)
		return nil
	}
	testTraceCase := &TestTraceCase{}
	if err = json.Unmarshal(data, testTraceCase); err != nil {
		t.Errorf("Read json %s, Failed, Error%v\n", dataFile, err)
		return nil
	}

	topology := NewTopology(apmType, testTraceCase.Services, map[string]*model.Trace{}, external.NewExternalFactory("topUrl"))
	relationships := make([]*Relationship, 0)
	for _, node := range topology.Nodes {
		relation := NewRelation(testTraceCase.TraceId, node)
		relation.CollectRelationships()
		relationships = append(relationships, relation.Relationships...)
	}
	return relationships
}

func fileExist(path string) bool {
	_, err := os.Stat(path)
	return os.IsNotExist(err) == false
}

func checkStringEqual(t *testing.T, prefix string, index int, key string, expect string, got string) {
	if expect != got {
		t.Errorf("[Check %s[%d].%s] want=%s, got=%s", prefix, index, key, expect, got)
	}
}

func checkIntEqual(t *testing.T, prefix string, key string, expect int, got int) {
	if expect != got {
		t.Errorf("[Check %s.%s] want=%d, got=%d", prefix, key, expect, got)
	}
}

func checkBoolEqual(t *testing.T, prefix string, index int, key string, expect bool, got bool) {
	if expect != got {
		t.Errorf("[Check %s[%d].%s] want=%t, got=%t", prefix, index, key, expect, got)
	}
}

type TestTraceCase struct {
	Name     string                      `json:"name"`
	TraceId  string                      `json:"traceId"`
	Services []*apmmodel.OtelServiceNode `json:"services"`
}
