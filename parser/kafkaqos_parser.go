package parser

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/linkedin/goavro"
	// "github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	// "github.com/qiniu/logkit/times"

	"github.com/wangtuanjie/ip17mon"
)

// const (
// 	KEY_SRC_IP   = "source_ip"
// 	KEY_METHOD   = "method"
// 	KEY_TOPIC    = "topic"
// 	KEY_CODE     = "code"
// 	KEY_RESP_LEN = "resp_len"
// 	KEY_DURATION = "duration"
// 	KEY_LOG_TIME = "log_time"
// 	KEY_ERROR    = "error"
// 	KEY_WARN     = "warn"
// 	EMPTY_STRING = ""
// )

type KafkaQosStreamParser struct {
	name    string
	domains []string
}

func (k *KafkaQosStreamParser) Name() string {
	return k.name
}

type Message struct {
	Service   string
	Topic     string
	Timestamp int64
	Body      string
}

type BaseEvent struct {
	Tag        string
	Method     string
	ClientIp   string
	Data       []string
	Device     string
	Country    string
	Region     string
	City       string
	Isp        string
	Stream     string
	OS         string
	ClientTime time.Time
	TimeStamp  int64
}

// MediaEvent struct
// see https://github.com/qbox/product/blob/master/pili.v2/SDKQOS/old_qos.v2.md
type MediaEvent struct {
	BaseEvent
	Scheme   string
	Domain   string
	Protocol string
	Path     string
	ReqID    string
	RemoteIP string
}

type StreamEvent struct {
	MediaEvent
	BeginAt                  time.Time
	EndAt                    time.Time
	VideoSourceFps           int64
	VideoSourceDroppedFrames int64
	AudioSourceFps           int64
	VideoEncodingFps         int64
	AudioEncodingFps         int64
	VideoSentFps             int64
	VideoBufferDroppedFrames int64
	AudioSentFps             int64
	AudioBitrate             int64
	VideoBitrate             int64
	VideoFilterTime          int64

	// start
	VideoEncodeType       string // ios264, droid264, x264, openh264, x265
	AudioEncodeType       string // iosaac, fdkaac, droidaac
	ExpectHaveVideoFrames int64  // 配置的video fps
	ExpectHaveAudioFrames int64  // 配置的audio fps
	GopTime               int64  // 两个关键帧的间隔长短 单位ms
	TcpConnect            int64  // tcp连接时间，ms
	RtmpConnect           int64  // rtmp 连接时间，ms

	//end

	VideoBufferSentFrames int64 // 视频发送帧总数
	AudioBufferSentFrames int64 // 音频发送帧总数

	DeviceModel       string  // 设备类型 iphone 4, ipad 2
	OsPlatform        string  // android ios
	OsVersion         string  // 10.0
	AppName           string  // android package id, ios bundle id
	AppVersion        string  // app 版本
	SysCpuUsage       float64 // [0..1] 结束时 发送总的平均值
	AppCpuUsage       float64 // [0..1] 结束时 发送总的平均值
	SysMemoryUsage    float64 // [0..1] 结束时 发送总的平均值
	AppMemoryUsage    float64 // [0..1] 结束时 发送总的平均值
	ComponentsVersion string  //librtmp-1.0.1;streamingkit-1.2;ffmpeg-3.0
	UiFps             int64   // 通过测试UI线程执行次数得到 UI 刷新速率，非准确值，仅用来参考 结束时发送总的平均值

	NetworkType string // None GPRS Edge WCDMA HSDPA UMTS HSUPA CDMA1x CDMAEVDORev0 CDMAEVDORevA CDMAEVDORevB eHRPD LTE WIFI

	IspName     string // mobile 网络情况下 运营商名字，wifi不填
	SignalDb    int64  // wifi 或 3G 信号强度，单位db
	SignalLevel int64  // 当获取不到db 时，使用 level 来表示，从0到8

	ErrorCode   int64 // 业务错误代码
	ErrorOscode int64 // 系统错误代码

}

// stream start
//106.9.76.231    stream_start.v5 1501764596557   319D7887-631E-4BF5-BD11-8316CAF91679    git-2017-04-28-09109eb1 rtmp    114.55.127.136  /megatest/p58954021501764595    mega1501764595  114.55.127.136  iosavh264       iosaac  15      44      3000    0       0       0

//stream
// 117.136.0.183   stream.v5       1501764521754   2B5CEBCC-E8B3-4566-A00D-408C7DABC657    git-2017-04-28-09109eb1 rtmp    114.55.126.152  /megatest/g14493372s3t1501764461049u4814907i14  mega1501764461  114.55.126.152  1501764461758   1501764521754   15      0       139     14      139     15      0       47      76913   169979  0.01

// stream end
// 106.9.76.231    stream_end.v5   1501764599305   319D7887-631E-4BF5-BD11-8316CAF91679    git-2017-04-28-09109eb1 rtmp    114.55.127.136  /megatest/p58954021501764595    mega1501764595  114.55.127.136  1501764596556   1501764599309   3000    22      0       116     0       11385   0       iPhone 6        ios     10.3.2  sdbean.MegaWerewolf     2.4.2   0       0.00    0       0.00    streamingkit-git-2017-04-28-09109eb1;librtmp-10004      47      LTE     10.76.231.21    222.222.222.222 -       中国电信              0       6       0       0       0       -       0       0
func buildTime(s string) time.Time {
	t, _ := strconv.ParseInt(s, 10, 64)
	return time.Unix(t/1000, t*1000000%1000000000)
}

func (krp *KafkaQosStreamParser) parseStreamStartEvent(data []string) (e *StreamEvent, err error) {
	if data == nil || len(data) < 17 {
		return nil, fmt.Errorf("not enough data")
	}
	if data[1] != "stream_start.v5" {
		return nil, fmt.Errorf("not stream")
	}
	event := StreamEvent{}
	event.ClientIp = data[0]
	event.Data = data
	event.Tag = data[1]
	event.ClientTime = buildTime(data[2])
	event.Device = data[3]
	event.Protocol = data[5]
	event.Domain = data[6]
	event.Path = data[7]
	event.ReqID = data[8]
	event.RemoteIP = data[9]
	event.VideoEncodeType = data[10]
	event.AudioEncodeType = data[11]
	event.ExpectHaveVideoFrames, _ = strconv.ParseInt(data[12], 10, 64)
	event.ExpectHaveAudioFrames, _ = strconv.ParseInt(data[13], 10, 64)
	event.GopTime, _ = strconv.ParseInt(data[14], 10, 64)
	// event.TcpConnect, _ = strconv.ParseInt(data[15], 10, 64)
	// event.RtmpConnect, _ = strconv.ParseInt(data[16], 10, 64)

	if strings.Contains(event.Device, "-") {
		event.OS = "iOS"
	} else {
		event.OS = "Android"
	}

	info, err := ip17mon.Find(event.ClientIp)
	if err != nil {
		return nil, fmt.Errorf("invalid ip")
	}
	event.Country = info.Country
	event.City = info.City
	event.Region = info.Region
	event.Isp = info.Isp
	return &event, nil
}

func (krp *KafkaQosStreamParser) parseStreamEndEvent(data []string) (e *StreamEvent, err error) {
	if data == nil || len(data) < 23 {
		return nil, fmt.Errorf("not enough data")
	}
	if data[1] != "stream_end.v5" {
		return nil, fmt.Errorf("not stream")
	}
	event := StreamEvent{}
	event.ClientIp = data[0]
	event.Data = data
	event.Tag = data[1]
	event.ClientTime = buildTime(data[2])
	event.Device = data[3]
	event.Protocol = data[5]
	event.Domain = data[6]
	event.Path = data[7]
	event.ReqID = data[8]
	event.RemoteIP = data[9]
	event.BeginAt = buildTime(data[10])
	event.EndAt = buildTime(data[11])
	event.GopTime, _ = strconv.ParseInt(data[12], 10, 64)
	event.VideoBufferSentFrames, _ = strconv.ParseInt(data[13], 10, 64)
	event.VideoBufferDroppedFrames, _ = strconv.ParseInt(data[14], 10, 64)
	event.AudioBufferSentFrames, _ = strconv.ParseInt(data[15], 10, 64)
	event.DeviceModel = data[19]
	event.OsPlatform = data[20]
	event.OsVersion = data[21]
	event.AppName = data[22]
	event.AppVersion = data[23]
	event.SysCpuUsage, _ = strconv.ParseFloat(data[24], 64)
	event.AppCpuUsage, _ = strconv.ParseFloat(data[25], 64)
	event.SysMemoryUsage, _ = strconv.ParseFloat(data[26], 64)
	event.AppMemoryUsage, _ = strconv.ParseFloat(data[27], 64)
	event.UiFps, _ = strconv.ParseInt(data[29], 10, 64)
	event.NetworkType = data[30]
	event.IspName = data[34]
	event.SignalDb, _ = strconv.ParseInt(data[35], 10, 64)
	event.SignalLevel, _ = strconv.ParseInt(data[36], 10, 64)
	event.ErrorCode, _ = strconv.ParseInt(data[41], 10, 64)
	event.ErrorOscode, _ = strconv.ParseInt(data[41], 10, 64)

	if strings.Contains(event.Device, "-") {
		event.OS = "iOS"
	} else {
		event.OS = "Android"
	}

	info, err := ip17mon.Find(event.ClientIp)
	if err != nil {
		return nil, fmt.Errorf("invalid ip")
	}
	event.Country = info.Country
	event.City = info.City
	event.Region = info.Region
	event.Isp = info.Isp
	return &event, nil
}

func (krp *KafkaQosStreamParser) parseStreamEvent(data []string) (e *StreamEvent, err error) {
	if data == nil || len(data) < 23 {
		return nil, fmt.Errorf("not enough data")
	}
	if data[1] != "stream.v5" {
		return nil, fmt.Errorf("not stream")
	}
	event := StreamEvent{}
	event.ClientIp = data[0]
	event.Data = data
	event.Tag = data[1]
	event.ClientTime = buildTime(data[2])
	event.Device = data[3]
	event.Protocol = data[5]
	event.Domain = data[6]
	event.Path = data[7]
	event.ReqID = data[8]
	event.RemoteIP = data[9]
	event.BeginAt = buildTime(data[10])
	event.EndAt = buildTime(data[11])
	event.VideoSourceFps, _ = strconv.ParseInt(data[12], 10, 64)
	event.VideoSourceDroppedFrames, _ = strconv.ParseInt(data[13], 10, 64)
	event.AudioSourceFps, _ = strconv.ParseInt(data[14], 10, 64)
	event.VideoEncodingFps, _ = strconv.ParseInt(data[15], 10, 64)
	event.AudioEncodingFps, _ = strconv.ParseInt(data[16], 10, 64)
	event.VideoSentFps, _ = strconv.ParseInt(data[17], 10, 64)
	event.VideoBufferDroppedFrames, _ = strconv.ParseInt(data[18], 10, 64)
	event.AudioSentFps, _ = strconv.ParseInt(data[19], 10, 64)
	event.AudioBitrate, _ = strconv.ParseInt(data[20], 10, 64)
	event.VideoBitrate, _ = strconv.ParseInt(data[21], 10, 64)
	event.VideoFilterTime, _ = strconv.ParseInt(data[22], 10, 64)

	if strings.Contains(event.Device, "-") {
		event.OS = "iOS"
	} else {
		event.OS = "Android"
	}

	info, err := ip17mon.Find(event.ClientIp)
	if err != nil {
		return nil, fmt.Errorf("invalid ip")
	}
	event.Country = info.Country
	event.City = info.City
	event.Region = info.Region
	event.Isp = info.Isp
	return &event, nil
}

func streamEventToSenderData(e *StreamEvent) sender.Data {
	d := sender.Data{}
	d["client_ip"] = e.ClientIp
	d["tag"] = e.Tag
	d["client_time"] = e.ClientTime
	d["device"] = e.Device
	d["protocol"] = e.Protocol
	d["domain"] = e.Domain
	d["path"] = e.Path
	d["reqid"] = e.ReqID
	d["remote_ip"] = e.RemoteIP
	d["os"] = e.OS
	d["country"] = e.Country
	d["city"] = e.City
	d["region"] = e.Region
	d["isp"] = e.Isp

	d["begin"] = e.BeginAt
	d["end"] = e.EndAt
	d["video_src_fps"] = e.VideoSourceFps
	d["video_src_dropped_frames"] = e.VideoSourceDroppedFrames
	d["audio_src_fps"] = e.AudioSourceFps
	d["video_enc_fps"] = e.VideoEncodingFps
	d["audio_enc_fps"] = e.AudioEncodingFps
	d["video_sent_fps"] = e.VideoSentFps
	d["video_buf_dropped_frames"] = e.VideoBufferDroppedFrames
	d["audio_sent_fps"] = e.AudioSentFps
	d["audio_bitrate"] = e.AudioBitrate
	d["video_bitrate"] = e.VideoBitrate
	d["video_filter_time"] = e.VideoFilterTime
	return d
}

func streamStartEventToSenderData(e *StreamEvent) sender.Data {
	d := sender.Data{}
	d["client_ip"] = e.ClientIp
	d["tag"] = e.Tag
	d["client_time"] = e.ClientTime
	d["device"] = e.Device
	d["protocol"] = e.Protocol
	d["domain"] = e.Domain
	d["path"] = e.Path
	d["reqid"] = e.ReqID
	d["remote_ip"] = e.RemoteIP
	d["os"] = e.OS
	d["country"] = e.Country
	d["city"] = e.City
	d["region"] = e.Region
	d["isp"] = e.Isp

	d["video_enc_type"] = e.VideoEncodeType
	d["audio_enc_type"] = e.AudioEncodeType
	d["expect_have_video_frames"] = e.ExpectHaveVideoFrames
	d["expect_have_audio_frames"] = e.ExpectHaveAudioFrames
	d["gop_time"] = e.GopTime

	return d
}

func streamEndEventToSenderData(e *StreamEvent) sender.Data {
	d := sender.Data{}
	d["client_ip"] = e.ClientIp
	d["tag"] = e.Tag
	d["client_time"] = e.ClientTime
	d["device"] = e.Device
	d["protocol"] = e.Protocol
	d["domain"] = e.Domain
	d["path"] = e.Path
	d["reqid"] = e.ReqID
	d["remote_ip"] = e.RemoteIP
	d["os"] = e.OS
	d["country"] = e.Country
	d["city"] = e.City
	d["region"] = e.Region
	d["isp"] = e.Isp

	d["begin"] = e.BeginAt
	d["end"] = e.EndAt
	d["gop_time"] = e.GopTime
	d["video_buf_sent_frames"] = e.VideoBufferSentFrames
	d["video_buf_dropped_frames"] = e.VideoBufferDroppedFrames
	d["audio_buf_sent_frames"] = e.AudioBufferSentFrames
	d["device_model"] = e.DeviceModel
	d["os_platform"] = e.OsPlatform
	d["os_version"] = e.OsVersion
	d["app_name"] = e.AppName
	d["app_version"] = e.AppVersion
	d["sys_cpu_usage"] = e.SysCpuUsage
	d["app_cpu_usage"] = e.AppCpuUsage
	d["sys_memory_usage"] = e.SysMemoryUsage
	d["app_memory_usage"] = e.AppMemoryUsage
	d["ui_fps"] = e.UiFps
	d["network_type"] = e.NetworkType
	d["isp_name"] = e.IspName
	d["signaldb"] = e.SignalDb
	d["signal_level"] = e.SignalLevel
	d["err_code"] = e.ErrorCode
	d["err_os_code"] = e.ErrorOscode

	return d
}

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

func (krp *KafkaQosStreamParser) Parse(lines []string) ([]sender.Data, error) {
	datas := []sender.Data{}
	for _, line := range lines {
		msg, err := decodeMessage([]byte(line))
		if msg == nil || err != nil {
			// fmt.Println(msg.Body)
			continue
		}
		if msg.Topic == "qos_raw_stream_v5" {
			data := strings.Split(msg.Body, "\t")
			if len(data) < 5 {
				continue
			}
			var e *StreamEvent
			var err error
			var dt sender.Data
			if data[1] == "stream.v5" {
				e, err = krp.parseStreamEvent(data)
				if err != nil || e == nil {
					continue
				}
				dt = streamEventToSenderData(e)
			} else if data[1] == "stream_start.v5" {
				e, err = krp.parseStreamStartEvent(data)
				if err != nil || e == nil {
					continue
				}
				dt = streamStartEventToSenderData(e)
			} else if data[1] == "stream_end.v5" {
				e, err = krp.parseStreamEndEvent(data)
				if err != nil || e == nil {
					continue
				}
				dt = streamEndEventToSenderData(e)
			} else {
				continue
			}
			for _, v := range krp.domains {
				if v == e.Domain {
					fmt.Println("parse domain", v, e.Domain)
					datas = append(datas, dt)
				}
			}
		}
	}
	return datas, nil
}

func NewKafkaQosStreamParser(c conf.MapConf) (LogParser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	ipDb, _ := c.GetStringOr("ipdb", "")
	err := ip17mon.Init(ipDb)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	domains, _ := c.GetStringOr("domains", "")
	domains = strings.TrimSpace(domains)
	domains2 := strings.Split(domains, ",")
	return &KafkaQosStreamParser{
		name:    name,
		domains: domains2,
	}, nil
}
