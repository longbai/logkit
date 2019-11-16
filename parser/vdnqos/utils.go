package vdnqos

import (
	"bytes"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"gopkg.in/linkedin/goavro.v1"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
	"strings"
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

func getDomainsAndHubs(apmHost, path string) (map[string]bool, map[string]string, error) {
	list, err := getDomainList(apmHost, path)
	if err != nil {
		return nil, nil, err
	}
	domains := map[string]bool{}
	hubs := map[string]string{}
	for _, v := range list {
		s := strings.Split(v, "/")
		domains[s[0]] = true
		if len(s) > 1 {
			hubs[s[1]] = s[0]
		}
	}
	return domains, hubs, nil
}

func domainIsIp(s string) bool {
	if len(s) < 7 {
		return false
	}
	x := []byte(s)
	c := int(x[len(x)-1])
	return c >= 48 && c <= 57
}

func getDomainList(apmHost, path string) ([]string, error) {
	resp, err := http.Get(apmHost + path)
	if err != nil {
		return nil, err
	}
	bytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var domains []string
	err = jsoniter.Unmarshal(bytes, &domains)
	if err != nil {
		return nil, err
	}
	return domains, err
}

func getDomains(apmHost, path string) (map[string]bool, error) {
	domains, err := getDomainList(apmHost, path)
	if err != nil {
		return nil, err
	}
	d := map[string]bool{}
	for _, v := range domains {
		d[v] = true
	}
	return d, nil
}

func IsAndroidDevice(device string) bool {
	isAndroidDevice := false
	if (len(device) > 13) {
		temp := device[0:13]
		timestamp, err := strconv.ParseInt(temp, 10, 64)
		if err != nil {
			return isAndroidDevice
		}
		timeLayout := "2006-01-02 15:04:05"                             //转化所需模板
		dataTimeStr := time.Unix(timestamp/1000, 0).Format(timeLayout) //设置时间戳 使用模板格式化为日期字符串
		_, err = time.Parse(timeLayout, dataTimeStr)
		if err == nil {
			isAndroidDevice = true
			return isAndroidDevice
		}

	}
	return isAndroidDevice
}
