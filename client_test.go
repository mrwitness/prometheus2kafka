
package main

import (
	"fmt"
	"os"
	"github.com/Shopify/sarama"
	"github.com/prometheus/common/model"
	"testing"
	"time"
	"./promadapter"
)

const (
	kafkaDefaultHost = "localhost"
	kafkaDefaultPort = "9092"
)

func strDefault(a, defaults string) string {
	if len(a) == 0 {
		return defaults
	}
	return a
}

func getenv(name,defaultValue string) string {
	return strDefault(os.Getenv(name), defaultValue)
}

func getConnectedClient() (*Client,error) {
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_1
	config.Producer.Return.Successes = true
	c := fmt.Sprintf("%v:%v","10.1.3.115",kafkaDefaultPort)
	hosts := []string{c}
	client,err := NewKafkaClient(hosts,config)
	if err != nil {
		return nil,err
	}
	err = client.Connect()
	return client,err
}

func TestSendModel(t *testing.T) {
	fmt.Println("TestSendModel")

	client,err := getConnectedClient()
	if err != nil {
		fmt.Println(err)
		return
	}

	var metricNameFixture = "rpc_widget_count"
	var valueFixture = 3.1415
	var timestampFixture = time.Now().UTC().Unix()
	var labels = model.LabelSet{
	        model.LabelName(model.MetricNameLabel): model.LabelValue(metricNameFixture),
       		model.LabelName("environment"):         model.LabelValue("production"),
   	        model.LabelName("job"):                 model.LabelValue("test"),
	}
	var promSamples = model.Samples{
	        &model.Sample{
           	        Metric:    model.Metric(labels),
               		Value:     model.SampleValue(valueFixture),
        	        Timestamp: model.Time(timestampFixture),
	        },
	}
	msgs := promadapter.SamplesToProducerMessages(promSamples)
	for _,msg := range msgs {
		fmt.Printf("begin to send message,topic: %s key: %s val:%s",msg.Topic,msg.Key,msg.Value)
                _,_,err := client.SendMessage(&msg)
                if err != nil {
			fmt.Println("SendMessage err:",err)
                }
        }
}

func TestTopic(t *testing.T) {
	fmt.Println("TestTopic")

	client,err := getConnectedClient()
	if err != nil {
		fmt.Println(err)	
		return
	}

	fmt.Println("connect success")

	topic := "topic1"
	exists,_ := client.IsTopicExsit(topic)
	fmt.Println("topic:",topic," exists: ",exists)
	
	err = client.CreateTopic(topic)
	if err != nil {
		fmt.Println("createTopic,err: ",err)
	} else {
		fmt.Println("createTopic success")
	}
}

func TestTwiceCreateTopic(t *testing.T) {
	fmt.Println("TestTwiceCreateTopic")
	client,err := getConnectedClient()
	if err != nil {
		fmt.Println(err)
		return
	}
	topic := "topic2"
	err = client.CreateTopic(topic)
	if err != nil {
		fmt.Println("createTopic,err: ",err)
	}
	// no err will be throw if topic is created twice
	err = client.CreateTopic(topic)
	if err != nil {
		fmt.Println("createTopic,err: ",err)
	}
}

func TestSendMessage(t *testing.T) {
	client,err := getConnectedClient()
	if err != nil {
		fmt.Println(err)
		return
	}
	msg := &sarama.ProducerMessage {
		Topic: "test",
		Key: sarama.ByteEncoder("hello"),
		Value: sarama.ByteEncoder("world"),
	}
	_,_,err = client.SendMessage(msg)
	if err != nil {
		fmt.Println("send message err: ",err)
	}
}

func getTestKafkaHost() string {
	return fmt.Sprintf("%v:%v",
		getenv("KAFKA_HOST",kafkaDefaultHost),
		getenv("KAFKA_PORT",kafkaDefaultPort),
	)
}


