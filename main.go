package main

import (
	"fmt"
	"os"
	"os/signal"
	"./config"
	"net/http"
	//"gopkg.in/Shopify/sarama.v1"
	"github.com/Shopify/sarama"
)

var osSignalChan = make(chan os.Signal,1)
var client *Client
func main() {
	signal.Notify(osSignalChan,os.Interrupt)
	go handleShutdown()
	
	port := config.BindPort()
	kHost := config.KafkaHost()
	kPort := config.KafkaPort()

	fmt.Println("bind port: ",port," kafka host: ",kHost," kafka port: ",kPort)
	kConfig := sarama.NewConfig()
	kConfig.Producer.Return.Successes = true
	
	//@ATTENTION 注意这里的kafka版本必须和线上的kafka版本一致
	kConfig.Version = sarama.V0_10_2_1 

	kConn := fmt.Sprintf("%v:%v",kHost,kPort)
	hosts := []string{kConn}
	var err error
	client,err = NewKafkaClient(hosts,kConfig)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = client.Connect()
	if err != nil {
		fmt.Println(err)
		return
	}
	http.Handle("/receive",receiveHandler(client))
	http.ListenAndServe(fmt.Sprintf(":%d",port),nil)
}

func handleShutdown() {
	fmt.Println("in handleShutdown..")
	<-osSignalChan
	fmt.Println("received stop signal")
	if client != nil {
		// not forget to close client....
		client.Close()
	}
	os.Exit(0)
}


