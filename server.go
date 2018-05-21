
package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"./promadapter"
	promremote "github.com/prometheus/prometheus/prompb"
)

func receiveHandler(client *Client) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter,r *http.Request) {
		fmt.Println("in receive url func,req is ",*r)
		compressed,err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w,err.Error(),http.StatusInternalServerError)
			return
		}
		data,err := processRequestData(compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		
		//promadapter.PromDataToProducerMessages(&data)
		msgs := promadapter.PromDataToProducerMessages(&data)

		for _,msg := range msgs {
			fmt.Printf("begin to send message,topic: %s key: %s val:%s",msg.Topic,msg.Key,msg.Value)
			_,_,err := client.SendMessage(&msg)
			if err != nil {
				fmt.Println("SendMessage err:",err)
			}
		} 

		w.WriteHeader(http.StatusAccepted)
	})
}

func processRequestData(reqBytes []byte) (promremote.WriteRequest,error) {
	var req promremote.WriteRequest
	reqBuf,err := snappy.Decode(nil,reqBytes)
	if err != nil {
		return req,err
	}

	if err := proto.Unmarshal(reqBuf,&req); err != nil {
		return req,err
	}
	return req,nil
}

