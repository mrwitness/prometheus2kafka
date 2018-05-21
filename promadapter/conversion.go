package promadapter

import (
	//"fmt"
	"github.com/prometheus/common/model"
	"github.com/Shopify/sarama"
	//"gopkg.in/Shopify/sarama.v1"
	"encoding/json"
	"strconv"
	promremote "github.com/prometheus/prometheus/prompb"
)

func PromDataToProducerMessages(req *promremote.WriteRequest) []sarama.ProducerMessage {
	return SamplesToProducerMessages(writeRequestToSamples(req))
}


// writeRequestToSamples converts a Prometheus remote storage WriteRequest to a collection of Prometheus common model Samples
func writeRequestToSamples(req *promremote.WriteRequest) model.Samples {
	var samples model.Samples
	for _, ts := range req.Timeseries {
		metric := make(model.Metric, len(ts.Labels))
		for _, label := range ts.Labels {
			metric[model.LabelName(label.Name)] = model.LabelValue(label.Value)
		}

		for _, sample := range ts.Samples {
			s := &model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(sample.Value),
				Timestamp: model.Time(sample.Timestamp),
			}
			samples = append(samples, s)
		}
	}
	return samples
}

func SamplesToProducerMessages(samples model.Samples) []sarama.ProducerMessage {
	var msgs []sarama.ProducerMessage
	for _,s := range samples {

		// here we simply use job as its topic
		topic := s.Metric[model.LabelName("job")]	
		s.Metric[model.LabelName("time")] = model.LabelValue(strconv.FormatUint(uint64(s.Timestamp),10))
		val := s.Value
		jKey,err := json.Marshal(labelsToTags(s))
		if err != nil {
			continue
		}
		msg := sarama.ProducerMessage{
			Topic: string(topic),
			Key:   sarama.ByteEncoder(jKey),
			Value: sarama.ByteEncoder(strconv.FormatUint(uint64(val),10)),
			//Timestamp: int64(s.Timestamp),
		}
		msgs = append(msgs,msg)
	}
	return msgs
}

// labelsToTags converts the Metric's associated Labels to AppOptics Tags
func labelsToTags(sample *model.Sample) map[string]string {
	var mt = make(map[string]string)
	for k, v := range sample.Metric {
		if k == model.MetricNameLabel {
			continue
		}
		mt[string(k)] = string(v)
	}
	return mt
}

