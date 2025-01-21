package external

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/CloudDetail/apo-module/apm/model/v1"
)

func TestOtelV1Middlewares(t *testing.T) {
	testMiddlewares(t, "otel-1.32.0", map[string][]*External{
		"http": {
			NewExternal(1730960127535392000, 3206786000, "5afb845bb9ebe7ab", "87909050f0110a2e", "external", "http", model.SpanKindClient, "GET /cpu", "localhost:9999", false, "http://localhost:9999/cpu/loop/1"),
		},
		"mysql": {
			NewExternal(1730960037021003000, 75991000, "", "8fc2daaf77bea002", "db", "mysql", model.SpanKindClient, "SELECT test.weather", "localhost:3306", false, "select count(?) from weather where temp_hi>?"),
			NewExternal(1730960037171531000, 2948000, "", "7db95a197465c5d4", "db", "mysql", model.SpanKindClient, "SELECT test.weather", "localhost:3306", false, "select id, city, prcpe from weather where temp_lo<=? and temp_hi>=?"),
		},
		"redis": {
			NewExternal(1730961966521214000, 5908000, "", "c0067407d121c5a7", "db", "redis", model.SpanKindClient, "SET", "localhost:6379", false, "SET aa ?"),
			NewExternal(1730961966382718000, 52620000, "", "a13257e2332f7dde", "db", "redis", model.SpanKindClient, "EXISTS", "localhost:6379", false, "EXISTS aa"),
		},
		"dubbo": {
			NewExternal(1730959641752503000, 965725000, "7e07b2ece6a18a58", "c33eeea5038d5daa", "external", "apache_dubbo", model.SpanKindClient, "io.apo.dubbo.api.service.OrderService/order2", "localhost:30002", false, "io.apo.dubbo.api.service.OrderService/order2"),
		},
		"grpc": {
			NewExternal(1730960380373063000, 2103665000, "f2b4a7bd8ae34934", "b0cdb06279ec4eae", "external", "grpc", model.SpanKindClient, "Greeter/SayHello", "localhost:9002", false, "Greeter/SayHello"),
		},
		"activemq": {
			NewExternal(1730960623274512000, 66282000, "aefa1c4fa3f8a3d4", "d71f931ffd9ee234", "mq", "jms", model.SpanKindProducer, "ActiveMQQueue", "", false, "ActiveMQQueue publish"),
			NewExternal(1730960623484000000, 15395000, "", "aefa1c4fa3f8a3d4", "mq", "jms", model.SpanKindConsumer, "ActiveMQQueue", "", false, "ActiveMQQueue process"),
		},
		"rabbitmq": {
			NewExternal(1730960813466295000, 25553000, "", "9b66d2e28e385c39", "mq", "rabbitmq", model.SpanKindClient, "exchange.declare", "localhost:5672", false, ""),
			NewExternal(1730960813492201000, 11396000, "", "3a6901e2be0434b7", "mq", "rabbitmq", model.SpanKindClient, "exchange.declare", "localhost:5672", false, ""),
			NewExternal(1730960813503879000, 12760000, "", "a20d6ff06c99e346", "mq", "rabbitmq", model.SpanKindClient, "queue.declare", "localhost:5672", false, ""),
			NewExternal(1730960813517662000, 15322000, "", "84b2ae986bcf3c72", "mq", "rabbitmq", model.SpanKindClient, "queue.bind", "localhost:5672", false, ""),
			NewExternal(1730960813541202000, 7558000, "98ae389821b7236c", "e78c558dea0d71fc", "mq", "rabbitmq", model.SpanKindProducer, "TestDirectExchange", "localhost:5672", false, "TestDirectExchange publish"),
			NewExternal(1730960816622000000, 395000, "", "ba4a05d889e68860", "mq", "rabbitmq", model.SpanKindConsumer, "TestDirectExchange", "", false, "TestDirectQueue process"),
			NewExternal(1730960816635000000, 55020000, "", "98ae389821b7236c", "mq", "rabbitmq", model.SpanKindConsumer, "TestDirectRouting", "", false, "TestDirectRouting process"),
		},
		"rocketmq": {
			NewExternal(1730961289385382000, 16725000, "44eb6d1e904911f3", "e52cf776d125a8f1", "mq", "rocketmq", model.SpanKindProducer, "cart-item-add-topic", "", false, "cart-item-add-topic publish"),
			NewExternal(1730961307212000000, 269084000, "", "44eb6d1e904911f3", "mq", "rocketmq", model.SpanKindConsumer, "cart-item-add-topic", "", false, "cart-item-add-topic process"),
		},
		"kafka": {
			NewExternal(1730961482041553000, 17667000, "0bbaca1352d8993a", "99aa616b8e73d37a", "mq", "kafka", model.SpanKindProducer, "topic_login", "", false, "topic_login publish"),
			NewExternal(1730961482061000000, 1434000, "", "0bbaca1352d8993a", "mq", "kafka", model.SpanKindConsumer, "topic_login", "", false, "topic_login process"),
		},
	})
}

func TestOtelV2Middlewares(t *testing.T) {
	testMiddlewares(t, "otel-2.9.0", map[string][]*External{
		"http": {
			NewExternal(1707269281528153000, 1541444000, "3073e355ddc1b8f6", "16fed07908c73983", "external", "http", model.SpanKindClient, "GET /cpu", "localhost:9999", false, "http://localhost:9999/cpu/loop/1"),
		},
		"error": {
			NewExternal(1707269289268482000, 12937000, "044eaa25409246f4", "3c50837b5ebd4199", "external", "http", model.SpanKindClient, "GET /wait", "localhost:9999", true, "http://localhost:9999/wait/fail"),
		},
		"mysql": {
			NewExternal(1730795478275216000, 7864000, "", "8dfe43d97b8c5225", "db", "mysql", model.SpanKindClient, "SELECT test.weather", "localhost:3306", false, "select count(?) from weather where temp_hi>?"),
			NewExternal(1730795478295825000, 553000, "", "85771ae9e0604460", "db", "mysql", model.SpanKindClient, "SELECT test.weather", "localhost:3306", false, "select id, city, prcpe from weather where temp_lo<=? and temp_hi>=?"),
		},
		"redis": {
			NewExternal(1730795484338703000, 772000, "", "df6b090361d8fa0e", "db", "redis", model.SpanKindClient, "SET", "localhost:6379", false, "SET bb ?"),
			NewExternal(1730795484318900000, 11698000, "", "7d37c9c2a7ac9ddb", "db", "redis", model.SpanKindClient, "EXISTS", "localhost:6379", false, "EXISTS bb"),
		},
		"dubbo": {
			NewExternal(1730807326430379000, 304018000, "acab4e23ebb1e64b", "23b65d878841b7fc", "external", "apache_dubbo", model.SpanKindClient, "io.apo.dubbo.api.service.OrderService/order2", "localhost:30002", false, "io.apo.dubbo.api.service.OrderService/order2"),
		},
		"grpc": {
			NewExternal(1707269776413106000, 631392000, "73a9e1a23971f5cd", "6924412511e95bbb", "external", "grpc", model.SpanKindClient, "Greeter/SayHello", "localhost:9002", false, "Greeter/SayHello"),
		},
		"activemq": {
			NewExternal(1730795308865385000, 29906000, "52c911f2ccbfbf36", "2556a6462986a211", "mq", "jms", model.SpanKindProducer, "ActiveMQQueue", "", false, "ActiveMQQueue publish"),
			NewExternal(1730795308899000000, 2474000, "", "52c911f2ccbfbf36", "mq", "jms", model.SpanKindConsumer, "ActiveMQQueue", "", false, "ActiveMQQueue process"),
		},
		"rabbitmq": {
			NewExternal(1730795028149338000, 3295000, "", "3eb721b51dab0e4f", "mq", "rabbitmq", model.SpanKindClient, "exchange.declare", "localhost:5672", false, ""),
			NewExternal(1730795028152743000, 7591000, "", "93ca0e89a76a2c02", "mq", "rabbitmq", model.SpanKindClient, "queue.declare", "localhost:5672", false, ""),
			NewExternal(1730795028160483000, 4908000, "", "c188ebaf97de0ee3", "mq", "rabbitmq", model.SpanKindClient, "queue.bind", "localhost:5672", false, ""),
			NewExternal(1730795028167098000, 1519000, "cfd0693a311e6ffb", "56594e21f1eee2ad", "mq", "rabbitmq", model.SpanKindProducer, "TestDirectExchange", "localhost:5672", false, "TestDirectExchange publish"),
			NewExternal(1730795028143250000, 5937000, "", "1bd4d4da0b897aab", "mq", "rabbitmq", model.SpanKindClient, "exchange.declare", "localhost:5672", false, ""),
			NewExternal(1730795038818000000, 10871000, "", "2ea9f55628592905", "mq", "rabbitmq", model.SpanKindConsumer, "TestDirectRouting", "", false, "TestDirectRouting process"),
			NewExternal(1730795038815000000, 115000, "", "cfd0693a311e6ffb", "mq", "rabbitmq", model.SpanKindConsumer, "TestDirectExchange", "localhost:5672", false, "TestDirectQueue process"),
		},
		"rocketmq": {
			NewExternal(1730794227958438000, 5673000, "168e101115ac2b96", "b995e7beff970862", "mq", "rocketmq", model.SpanKindProducer, "cart-item-add-topic", "", false, "cart-item-add-topic publish"),
			NewExternal(1730794227970000000, 457000, "", "168e101115ac2b96", "mq", "rocketmq", model.SpanKindConsumer, "cart-item-add-topic", "", false, "cart-item-add-topic process"),
		},
		"kafka": {
			NewExternal(1730793912574554000, 4268000, "ec50f3a414b0e272", "e27b36d8a6336b50", "mq", "kafka", model.SpanKindProducer, "topic_login", "", false, "topic_login publish"),
			NewExternal(1730793912580000000, 578000, "", "ec50f3a414b0e272", "mq", "kafka", model.SpanKindConsumer, "topic_login", "", false, "topic_login process"),
		},
	})
}

func TestSkywalkingMiddlewares(t *testing.T) {
	testMiddlewares(t, "skywalking", map[string][]*External{
		"http": {
			NewExternal(1706168392719000000, 1563000000, "63b6612e797615a7", "3e965182dba01b2b", "external", "http", model.SpanKindClient, "GET /db", "localhost:9999", false, "http://localhost:9999/db/query"),
			NewExternal(1706168393558000000, 100000000, "", "606b9c2d647615a7", "db", "mysql", model.SpanKindClient, "SELECT test.weather", "localhost:3306", false, "select city, temp_lo, temp_hi, prcpe from weather where temp_lo<=? and temp_hi>=?"),
			NewExternal(1706168393671000000, 2000000, "", "676b9c2d647615a7", "db", "mysql", model.SpanKindClient, "SELECT test.weather", "localhost:3306", false, "select city, temp_lo, temp_hi, prcpe from weather where temp_lo<=? and temp_hi>=?"),
		},
		"error": {
			NewExternal(1706597117363000000, 998000000, "4a24251a957f2bf3", "64e57d30dcfb44d1", "external", "http", model.SpanKindClient, "GET /wait", "localhost:9999", true, "http://localhost:9999/wait/fail"),
		},
		"redis": {
			NewExternal(1730791981640000000, 21000000, "", "8017319bec6eda42", "db", "redis", model.SpanKindClient, "Lettuce/EXISTS", "localhost:6379", false, "EXISTS"),
			NewExternal(1730791981670000000, 2000000, "", "8317319bec6eda42", "db", "redis", model.SpanKindClient, "Lettuce/SET", "localhost:6379", false, "SET"),
		},
		"dubbo": {
			NewExternal(1706164669451000000, 536000000, "627391567b7849b8", "99d26201482ece3e", "external", "dubbo", model.SpanKindClient, "io.apo.dubbo.api.service.OrderService.order2(long,long)", "localhost:30002", false, "dubbo://localhost:30002/io.apo.dubbo.api.service.OrderService.order2(long,long)"),
		},
		"grpc": {
			NewExternal(1730811346638000000, 124000000, "dd95ac295ab9a7dd", "c4d652e60b91ad8c", "external", "grpc", model.SpanKindClient, "Greeter.sayHello", "springboot-grpc-server", false, "Greeter.sayHello"),
		},
		"activemq": {
			NewExternal(1730778284548000000, 52000000, "9dc0ee47267c4446", "fa9245dd1fef6738", "mq", "activemq", model.SpanKindProducer, "ActiveMQQueue", "localhost:61616", false, "ActiveMQ/Queue/ActiveMQQueue/Producer"),
			NewExternal(1730778284680000000, 20000000, "", "9dc0ee47267c4446", "mq", "activemq", model.SpanKindConsumer, "ActiveMQQueue", "localhost:61616", false, "ActiveMQ/Queue/ActiveMQQueue/Consumer"),
		},
		"rabbitmq": {
			NewExternal(1730777843115000000, 1000000, "86ea561347b29f7f", "fe775e9480c9d861", "mq", "rabbitmq", model.SpanKindProducer, "TestDirectRouting", "localhost:5672", false, "RabbitMQ/Topic/TestDirectExchangeQueue/TestDirectRouting/Producer"),
			NewExternal(1730777843119000000, 0, "", "86ea561347b29f7f", "mq", "rabbitmq", model.SpanKindConsumer, "TestDirectRouting", "localhost:5672", false, "RabbitMQ/Topic/TestDirectExchangeQueue/TestDirectRouting/Consumer"),
		},
		"rocketmq": {
			NewExternal(1730778870713000000, 1961000000, "2875268e3fbeb894", "664569984ff43c17", "mq", "rocketmq", model.SpanKindProducer, "cart-item-add-topic", "localhost:10911", false, "RocketMQ/cart-item-add-topic/Producer"),
			NewExternal(1730778873163000000, 1000000, "", "2875268e3fbeb894", "mq", "rocketmq", model.SpanKindConsumer, "cart-item-add-topic", "localhost:10911", false, "RocketMQ/cart-item-add-topic/Consumer"),
		},
		"kafka": {
			NewExternal(1730793356907000000, 1000000, "cc5127d5787df687", "ddded06df9ae8e79", "mq", "kafka", model.SpanKindProducer, "topic_login", "localhost:9092", false, "Kafka/topic_login/Producer"),
			NewExternal(1730793356916000000, 1000000, "", "cc5127d5787df687", "mq", "kafka", model.SpanKindConsumer, "topic_login", "localhost:9092", false, "Kafka/topic_login/Consumer/group1"),
		},
	})
}

func TestUnknownMiddlewares(t *testing.T) {
	testMiddlewares(t, "unknown", map[string][]*External{
		"http": {
			NewExternal(1730960127535392000, 3206786000, "5afb845bb9ebe7ab", "87909050f0110a2e", "external", "unknown", model.SpanKindClient, "GET", "", false, ""),
		},
		"mysql": {
			NewExternal(1730960037021003000, 75991000, "", "8fc2daaf77bea002", "external", "unknown", model.SpanKindClient, "SELECT test.weather", "", false, ""),
		},
		"redis": {
			NewExternal(1730961966521214000, 5908000, "", "c0067407d121c5a7", "external", "unknown", model.SpanKindClient, "SET", "", false, ""),
		},
		"dubbo": {
			NewExternal(1730959641752503000, 965725000, "7e07b2ece6a18a58", "c33eeea5038d5daa", "external", "unknown", model.SpanKindClient, "io.apo.dubbo.api.service.OrderService/order2", "", false, ""),
		},
		"grpc": {
			NewExternal(1730960380373063000, 2103665000, "f2b4a7bd8ae34934", "b0cdb06279ec4eae", "external", "unknown", model.SpanKindClient, "Greeter/SayHello", "springboot-grpc-server", false, ""),
		},
		"activemq": {
			NewExternal(1730960623274512000, 66282000, "aefa1c4fa3f8a3d4", "d71f931ffd9ee234", "mq", "unknown", model.SpanKindProducer, "unknown", "", false, "ActiveMQQueue publish"),
			NewExternal(1730960623484000000, 15395000, "", "aefa1c4fa3f8a3d4", "mq", "unknown", model.SpanKindConsumer, "unknown", "", false, "ActiveMQQueue process"),
		},
		"rabbitmq": {
			NewExternal(1730960813466295000, 25553000, "", "9b66d2e28e385c39", "external", "unknown", model.SpanKindClient, "exchange.declare", "", false, ""),
			NewExternal(1730960813492201000, 11396000, "", "3a6901e2be0434b7", "external", "unknown", model.SpanKindClient, "exchange.declare", "", false, ""),
			NewExternal(1730960813503879000, 12760000, "", "a20d6ff06c99e346", "external", "unknown", model.SpanKindClient, "queue.declare", "", false, ""),
			NewExternal(1730960813517662000, 15322000, "", "84b2ae986bcf3c72", "external", "unknown", model.SpanKindClient, "queue.bind", "", false, ""),
			NewExternal(1730960813541202000, 7558000, "98ae389821b7236c", "e78c558dea0d71fc", "mq", "unknown", model.SpanKindProducer, "unknown", "", false, "TestDirectExchange publish"),
			NewExternal(1730960816622000000, 395000, "", "ba4a05d889e68860", "mq", "unknown", model.SpanKindConsumer, "unknown", "", false, "TestDirectQueue process"),
			NewExternal(1730960816635000000, 55020000, "", "98ae389821b7236c", "mq", "unknown", model.SpanKindConsumer, "unknown", "", false, "TestDirectRouting process"),
		},
		"rocketmq": {
			NewExternal(1730961289385382000, 16725000, "44eb6d1e904911f3", "e52cf776d125a8f1", "mq", "unknown", model.SpanKindProducer, "unknown", "", false, "cart-item-add-topic publish"),
			NewExternal(1730961307212000000, 269084000, "", "44eb6d1e904911f3", "mq", "unknown", model.SpanKindConsumer, "unknown", "", false, "cart-item-add-topic process"),
		},
		"kafka": {
			NewExternal(1730961482041553000, 17667000, "0bbaca1352d8993a", "99aa616b8e73d37a", "mq", "unknown", model.SpanKindProducer, "unknown", "", false, "topic_login publish"),
			NewExternal(1730961482061000000, 1434000, "", "0bbaca1352d8993a", "mq", "unknown", model.SpanKindConsumer, "unknown", "", false, "topic_login process"),
		},
	})
}

func testMiddlewares(t *testing.T, apmType string, data map[string][]*External) {
	for testCase, expects := range data {
		testClientCase := buildExternalDatas(t, apmType, testCase)
		if testClientCase != nil {
			checkIntEqual(t, "Datas Size", len(expects), len(testClientCase))
			for i, expect := range expects {
				got := testClientCase[i]

				checkUint64Equal(t, "startTime", expect.StartTime, got.StartTime)
				checkUint64Equal(t, "duration", expect.Duration, got.Duration)
				checkStringEqual(t, fmt.Sprintf("%s.spanId", testCase), expect.SpanId, got.SpanId)
				checkStringEqual(t, fmt.Sprintf("%s.next", testCase), expect.NextSpanId, got.NextSpanId)
				checkStringEqual(t, "group", expect.Group, got.Group)
				checkStringEqual(t, "type", expect.Type, got.Type)
				checkIntEqual(t, "kind", int(expect.Kind), int(got.Kind))
				checkStringEqual(t, "name", expect.Name, got.Name)
				checkStringEqual(t, "peer", expect.Peer, got.Peer)
				checkBoolEqual(t, "error", expect.Error, got.Error)
				checkStringEqual(t, "detail", expect.Detail, got.Detail)
			}
		}
	}
}

func buildExternalDatas(t *testing.T, apmType string, testCase string) []*External {
	dataFile := fmt.Sprintf("testdata/%s/%s.json", apmType, testCase)
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

	result := make([]*External, 0)
	collectExternalDatas(&result, testTraceCase.Services)
	return result
}

func fileExist(path string) bool {
	_, err := os.Stat(path)
	return os.IsNotExist(err) == false
}

func checkStringEqual(t *testing.T, key string, expect string, got string) {
	if expect != got {
		t.Errorf("[Check %s] want=%s, got=%s", key, expect, got)
	}
}

func checkIntEqual(t *testing.T, key string, expect int, got int) {
	if expect != got {
		t.Errorf("[Check %s] want=%d, got=%d", key, expect, got)
	}
}

func checkUint64Equal(t *testing.T, key string, expect uint64, got uint64) {
	if expect != got {
		t.Errorf("[Check %s] want=%d, got=%d", key, expect, got)
	}
}

func checkBoolEqual(t *testing.T, key string, expect bool, got bool) {
	if expect != got {
		t.Errorf("[Check %s] want=%t, got=%t", key, expect, got)
	}
}

func collectExternalDatas(clientDatas *[]*External, services []*model.OtelServiceNode) {
	factory := NewExternalFactory("topUrl")
	for _, service := range services {
		*clientDatas = append(*clientDatas, factory.BuildExternals(service)...)
		collectExternalDatas(clientDatas, service.Children)
	}
}

type TestTraceCase struct {
	Name     string                   `json:"name"`
	TraceId  string                   `json:"traceId"`
	Services []*model.OtelServiceNode `json:"services"`
}
