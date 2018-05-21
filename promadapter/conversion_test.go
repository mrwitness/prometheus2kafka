package promadapter

import (
	"fmt"
	"github.com/prometheus/common/model"
	"testing"
	"time"
	"encoding/json"
)

var metricNameFixture = "rpc_widget_count"
var valueFixture = 3.1415
var timestampFixture = time.Now().UTC().Unix()

var labels = model.LabelSet{
	model.LabelName(model.MetricNameLabel): model.LabelValue(metricNameFixture),
	model.LabelName("environment"):         model.LabelValue("production"),
	model.LabelName("job"):                 model.LabelValue("inventory-service"),
}

var promSamples = model.Samples{
	&model.Sample{
		Metric:    model.Metric(labels),
		Value:     model.SampleValue(valueFixture),
		Timestamp: model.Time(timestampFixture),
	},
}

func TestSamplesToMeasurements(t *testing.T) {
	fmt.Println("TestSamplesToMeasurements")
	ms := SamplesToProducerMessages(promSamples)

	for _, msg := range ms {
		fmt.Println("topic: ",msg.Topic)
		fmt.Printf("key: %s",msg.Key)
		fmt.Printf("value: %s",msg.Value)

		fmt.Println(msg.Timestamp)
	}
}

func TestModel(t *testing.T) {
	fmt.Println("TestModel")
	metric := make(model.Metric, 2)
	metric["key1"] = "key1"
	metric["key2"] = "key2"
	fmt.Println(metric)
}

func TestMapMarshal(t *testing.T) {
	fmt.Println("TestMapMarshal")
	
	x := make(map[string]string)
	x["hello"] = "wong"
	x["hello2"] = "doc strange"
	jX,_ := json.Marshal(x)	
	fmt.Printf("after marshal,%s ",jX)

}


