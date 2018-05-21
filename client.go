package main

import (
	"fmt"
	//"gopkg.in/Shopify/sarama.v1"
	"github.com/Shopify/sarama"
	"time"
)

type Client struct {
	hosts []string
	config sarama.Config
	sproducer sarama.SyncProducer
	client sarama.Client
}

func NewKafkaClient(
	hosts []string,
	cfg *sarama.Config,
)(*Client, error) {
	c := &Client{
		hosts: hosts,
		config: *cfg,
	}
	return c, nil
}

func (c *Client) Connect() error {

	producer,err := sarama.NewSyncProducer(c.hosts,&c.config)
	if err != nil {
		fmt.Printf("err: %s",err)
		return err
	}
	client,err := sarama.NewClient(c.hosts,&c.config)
	if err != nil {
		fmt.Printf("err: %s",err)
		return err
	}
	c.client = client
	c.sproducer = producer
	return nil
}

func (c *Client) Close() error {
	err := c.sproducer.Close()
	if err != nil {
		return err
	}
	c.sproducer = nil
	brokers := c.client.Brokers()
	for _,b := range brokers {
		if connected, _ := b.Connected(); connected {
			b.Close()
		}
	}
	c.client.Close()
	return nil
}


func (c *Client) SendMessages(msgs []*sarama.ProducerMessage) error {

	for _,msg := range msgs {
		topic := (*msg).Topic
		exists,err := c.IsTopicExsit(topic)
		if err != nil {
			return err
		}
		if exists == false {
			c.CreateTopic(topic)
		}
	}
	return c.sproducer.SendMessages(msgs)
}

//check topic first before send any messages...
func (c *Client) SendMessage(msg *sarama.ProducerMessage) (int32,int64,error) {
	topic := (*msg).Topic
	exists,err := c.IsTopicExsit(topic)
	if err != nil {
		return 0,0,err
	}		

	if exists == false {
		c.CreateTopic(topic)
	}
	return c.sproducer.SendMessage(msg)
}


func (c *Client) IsTopicExsit(topic string) (bool,error) {
	topics,err := c.client.Topics()
	if err != nil {
		return false,err
	}

	for _,t := range topics {
		if t == topic {
			return true,nil
		}
	}
	return false,nil
}

func (c *Client) CreateTopic(topic string) error {
	broker := c.client.Brokers()[0]
	
	if connected, _ := broker.Connected(); !connected {
		err := broker.Open(&c.config)
		if err != nil {
			return err
		}
	}

	retention := "-1"
	req := &sarama.CreateTopicsRequest {
		TopicDetails: map[string]*sarama.TopicDetail{
			topic: {
				NumPartitions: -1,
				ReplicationFactor: -1,
				ReplicaAssignment: map[int32][]int32{
					0: []int32{0, 1, 2},
				},
				ConfigEntries: map[string]*string{
					"retention.ms": &retention,
				},
			},
		},
		Timeout: 100 * time.Millisecond,

	}
	_,err := broker.CreateTopics(req)
	//fmt.Println("createTopic rsp: ",rsp)
	return err
}

