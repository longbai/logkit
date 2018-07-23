package vdnqos

import (
	"fmt"
	"strconv"
	goavro "gopkg.in/linkedin/goavro.v1"
	"bytes"
)

func decodeMessage(message []byte) (*Message, error) {
	codec, err := goavro.NewCodec("{\"type\":\"record\",\"name\":\"AvroFlumeEvent\",\"namespace\":\"org.apache.flume.source.avro\",\"fields\":[{\"name\":\"headers\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"body\",\"type\":\"bytes\"}]}")
	if err != nil {
		panic(err)
	}
	msg := new(Message)
	datum, err := codec.Decode(bytes.NewReader(message))
	if err != nil {
		return nil, err
	}
	decodedRecord, ok := datum.(*goavro.Record)
	if !ok {
		return nil, fmt.Errorf("expected *goavro.Record; received: %T\n", decodedRecord)
	}
	headers, err := decodedRecord.Get("headers")
	if err != nil {
		return nil, err
	}
	header, ok := headers.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("expected *goavro.Record; received: %T\n", header)
	}
	service, ok := header["service"].(string)
	if !ok {
		return nil, fmt.Errorf("comma-ok err: %#v %T\n", header["service"], header["service"])
	}
	// Service assign
	msg.Service = service
	topic, ok := header["topic"].(string)
	if !ok {
		return nil, fmt.Errorf("comma-ok err: %#v %T\n", header["topic"], header["topic"])
	}
	// Topic assign
	msg.Topic = topic
	timestamp, ok := header["timestamp"].(string)
	if !ok {
		return nil, fmt.Errorf("comma-ok err: %#v %T\n", header["timestamp"], header["timestamp"])
	}
	i, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return nil, err
	}
	// Timestamp assign
	msg.Timestamp = i
	body, err := decodedRecord.Get("body") // []byte
	if err != nil {
		return nil, err
	}
	// Body assign
	msg.Body = fmt.Sprintf("%s", body)
	return msg, nil
}
