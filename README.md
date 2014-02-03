# .Net Kafka Client

This is an _experimental_ Kafka Client for .Net.

**Apache Kafka 0.8+ support:**

  * Branch: master
  * Producer: __NOT__ supported
  * SimpleConsumer: __somewhat__ supported 
  * Rebalancing Consumer: __NOT__ supported
  * Compression: __NOT__ supported
  * ZooKeeper: __NOT__ supported
  * Status: Experimental, under development

Plan is to start implement SimpleConsumer, Producer, Compression.

```c#
using Kafka.Client;

var kafkaClient = new KafkaClient("127.0.0.1", 9092, "DemoClient");

var consumer = new SimpleConsumer(kafkaClient, topic: "test");
foreach(var message in consumer.GetMessages())
{
	  //Consume the message
}
```
