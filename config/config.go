
package config

import (
	"flag"
)

var bindPort int

var kafkaHost string
var kafkaPort int

var globalConf *Config

func init() {
	flag.IntVar(&bindPort,"bind-port",1234,"the port the HTTP server binds to")
	flag.StringVar(&kafkaHost,"kafka-host","127.0.0.1","kafka host")
	flag.IntVar(&kafkaPort,"kafka-port",9092,"kafka port")

	flag.Parse()
	globalConf = New()
}

type Config struct {
	bindPort int
	kafkaHost string
	kafkaPort int
}

func New() *Config {
	return &Config {
		bindPort: bindPort,
		kafkaHost: kafkaHost,
		kafkaPort: kafkaPort,
	}
}

func BindPort() int {
	return globalConf.bindPort
}

func KafkaHost() string {
	return globalConf.kafkaHost
}

func KafkaPort() int {
	return globalConf.kafkaPort
}

