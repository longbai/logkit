package vdnqos

import (
	// "bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	// "github.com/linkedin/goavro"
	// "github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	// "github.com/qiniu/logkit/sender"
	// "github.com/qiniu/logkit/times"

	"github.com/wangtuanjie/ip17mon"

	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/utils/models"

	"github.com/json-iterator/go"
	"github.com/qiniu/log"
	"io/ioutil"
	"net/http"
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

func init() {
	parser.RegisterConstructor(parser.TypeKafkaQosPlay, NewKafkaQosPlayParser)
}

type KafkaQosPlayParser struct {
	name                   string
	domains                []string
	apmHost                string
	playDomainRetrievePath string
}

func (k *KafkaQosPlayParser) Name() string {
	return k.name
}

func (k *KafkaQosPlayParser) RefreshDomains() {
	ticker := time.NewTicker(time.Minute * 5)
	go func() {
		for range ticker.C {
			domains, err := getDomains(k.apmHost, k.playDomainRetrievePath)
			if err != nil {
				log.Error(err)
			}
			k.domains = domains
			log.Info("successfully updated play domains to %v", domains)
		}
	}()
}

type PlayEvent struct {
	MediaEvent
	BeginAt time.Time
	EndAt   time.Time
	//play
	Buffering                int64
	VideoSourceFps           int64
	VideoSourceDroppedFrames int64
	AudioSourceFps           int64
	AudioSourceDroppedFrames int64
	VideoRenderFps           int64
	AudioRenderFps           int64
	VideoBufferSize          int64
	AudioBufferSize          int64
	AudioBitrate             int64
	VideoBitrate             int64

	// start
	FirstVideoTime  int64
	FirstAudioTIme  int64
	VideoDecodeType string // ios264, droid264, x264, openh264, x265
	AudioDecodeType string // iosaac, fdkaac, droidaac
	GopTime         int64  // 两个关键帧的间隔长短 单位ms

	//end
	BufferingTotalCount int64
	BufferingTotalTime  int64
	TotalRecvBytes      int64
	EndBufferingTime    int64

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

//222.188.168.212	play.v5	1504250399319	1503281015004993	1.1.0.32	rtmp	pull.lespark.cn	live/57762d4b245bfa685f92af03	-	61.160.199.165	1504250339036	1504250399319	0	14.35	0	47.00	0	13.34	37.22	3342	3968	97199	351830

func (krp *KafkaQosPlayParser) parsePlayEvent(data []string) (e *PlayEvent, err error) {
	if data == nil || len(data) < 23 {
		return nil, fmt.Errorf("not enough data")
	}
	if data[1] != "play.v5" {
		return nil, fmt.Errorf("not stream")
	}
	event := PlayEvent{}
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

	event.Buffering, _ = strconv.ParseInt(data[12], 10, 64)
	event.VideoSourceFps, _ = strconv.ParseInt(data[13], 10, 64)
	event.VideoSourceDroppedFrames, _ = strconv.ParseInt(data[14], 10, 64)
	event.AudioSourceFps, _ = strconv.ParseInt(data[15], 10, 64)
	event.AudioSourceDroppedFrames, _ = strconv.ParseInt(data[16], 10, 64)
	event.VideoRenderFps, _ = strconv.ParseInt(data[17], 10, 64)
	event.AudioRenderFps, _ = strconv.ParseInt(data[18], 10, 64)
	event.VideoBufferSize, _ = strconv.ParseInt(data[19], 10, 64)
	event.AudioBufferSize, _ = strconv.ParseInt(data[20], 10, 64)
	event.AudioBitrate, _ = strconv.ParseInt(data[21], 10, 64)
	event.VideoBitrate, _ = strconv.ParseInt(data[22], 10, 64)

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

//112.96.173.42	play_start.v5	1504250339646	1501257229489135	1.5.1	http	114.55.127.136	/g15695073s0t1504250339644u5953981i17.flv	-	114.55.127.136	3615	3698	1002	ffmpeg	ffmpeg	0	0

func (krp *KafkaQosPlayParser) parsePlayStartEvent(data []string) (e *PlayEvent, err error) {
	if data == nil || len(data) < 16 {
		return nil, fmt.Errorf("not enough data")
	}
	if data[1] != "play_start.v5" {
		return nil, fmt.Errorf("not stream")
	}
	event := PlayEvent{}
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

	event.FirstVideoTime, _ = strconv.ParseInt(data[10], 10, 64)
	event.FirstAudioTIme, _ = strconv.ParseInt(data[11], 10, 64)

	event.GopTime, _ = strconv.ParseInt(data[12], 10, 64)

	event.VideoDecodeType = data[13]
	event.AudioDecodeType = data[14]

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

//171.41.69.17	play_end.v5	1504250401045	1503145461301629	1.5.1	http	flv.live-baidu.rela.me	/live/101059696.flv	-	flv.live-baidu.rela.me	1504250386897	1504250401045	0	0 1559514	0	2999	vivo_X9	Android	6.0.1	com.thel	4.0.2	0.378	0.178	0.630	0.052	ffmpeg-3.2;PLDroidPlayer-1.5.1	60	WIFI	192.168.1.103	8.8.8.8	"luckywan"	-	-6300	0	0	-	0	0
//112.96.173.42	play_end.v5	1504250395823	1501257229489135	1.5.1	http	114.55.127.136	/g15695073s0t1504250339644u5953981i17.flv	-	114.55.127.136	1504250335947	1504250395822	0	0	1822465	0	1002	XiaomiMI_MAX_2	Android	7.1.1	com.sdbean.werewolf	2.07	0.325	0.133	0.777	0.037	ffmpeg-3.2;PLDroidPlayer-1.5.1	60	LTE	10.179.133.142	221.5.88.88	-	-	0	0	0	0	0	-	0	0
func (krp *KafkaQosPlayParser) parsePlayEndEvent(data []string) (e *PlayEvent, err error) {
	if data == nil || len(data) < 37 {
		return nil, fmt.Errorf("not enough data")
	}
	if data[1] != "play_end.v5" {
		return nil, fmt.Errorf("not stream")
	}
	event := PlayEvent{}
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

	event.BufferingTotalCount, _ = strconv.ParseInt(data[12], 10, 64)
	event.BufferingTotalTime, _ = strconv.ParseInt(data[13], 10, 64)
	event.TotalRecvBytes, _ = strconv.ParseInt(data[14], 10, 64)
	event.EndBufferingTime, _ = strconv.ParseInt(data[15], 10, 64)
	event.GopTime, _ = strconv.ParseInt(data[16], 10, 64)

	event.DeviceModel = data[17]
	event.OsPlatform = data[18]
	event.OsVersion = data[19]
	event.AppName = data[20]
	event.AppVersion = data[21]
	event.SysCpuUsage, _ = strconv.ParseFloat(data[22], 64)
	event.AppCpuUsage, _ = strconv.ParseFloat(data[23], 64)
	event.SysMemoryUsage, _ = strconv.ParseFloat(data[24], 64)
	event.AppMemoryUsage, _ = strconv.ParseFloat(data[25], 64)
	event.UiFps, _ = strconv.ParseInt(data[26], 10, 64)
	event.NetworkType = data[27]
	event.IspName = data[28]
	event.SignalDb, _ = strconv.ParseInt(data[29], 10, 64)
	event.SignalLevel, _ = strconv.ParseInt(data[30], 10, 64)
	event.ErrorCode, _ = strconv.ParseInt(data[35], 10, 64)
	event.ErrorOscode, _ = strconv.ParseInt(data[36], 10, 64)

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

func playEventToSenderData(e *PlayEvent) models.Data {
	d := models.Data{}
	d["client_ip"] = e.ClientIp
	d["tag"] = e.Tag
	d["client_time"] = e.ClientTime.Format(time.RFC3339)
	d["server_time"] = time.Unix(e.TimeStamp/1000, 0).Format(time.RFC3339)
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

	d["buffering"] = e.Buffering
	d["video_src_fps"] = e.VideoSourceFps
	d["audio_src_fps"] = e.AudioSourceFps
	d["video_render_fps"] = e.VideoRenderFps
	d["audio_render_fps"] = e.AudioRenderFps
	d["video_buf_size"] = e.VideoBufferSize
	d["audio_buf_size"] = e.AudioBufferSize
	d["audio_bitrate"] = e.AudioBitrate
	d["video_bitrate"] = e.VideoBitrate
	return d
}

func playStartEventToSenderData(e *PlayEvent) models.Data {
	d := models.Data{}
	d["client_ip"] = e.ClientIp
	d["tag"] = e.Tag
	d["client_time"] = e.ClientTime.Format(time.RFC3339)
	d["server_time"] = time.Unix(e.TimeStamp/1000, 0).Format(time.RFC3339)
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

	d["first_video_time"] = e.FirstVideoTime
	d["first_audio_time"] = e.FirstAudioTIme
	d["video_decode_type"] = e.VideoDecodeType
	d["audio_decode_type"] = e.AudioDecodeType
	d["gop_time"] = e.GopTime

	return d
}

func playEndEventToSenderData(e *PlayEvent) models.Data {
	d := models.Data{}
	d["client_ip"] = e.ClientIp
	d["tag"] = e.Tag
	d["client_time"] = e.ClientTime.Format(time.RFC3339)
	d["server_time"] = time.Unix(e.TimeStamp/1000, 0).Format(time.RFC3339)
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

	d["buffering_total_count"] = e.BufferingTotalCount
	d["buffering_total_time"] = e.BufferingTotalTime
	d["end_buffering_time"] = e.EndBufferingTime

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

func (krp *KafkaQosPlayParser) Parse(lines []string) ([]models.Data, error) {
	datas := []models.Data{}
	for _, line := range lines {
		msg, err := decodeMessage([]byte(line))
		if msg == nil || err != nil {
			// fmt.Println(msg.Body)
			continue
		}
		if msg.Topic == "qos_raw_play_v5" || msg.Topic == "qos_raw_misc_v5_vod" {
			data := strings.Split(msg.Body, "\t")
			if len(data) < 5 {
				continue
			}
			var e *PlayEvent
			var err error
			var dt models.Data
			if data[1] == "play.v5" {
				e, err = krp.parsePlayEvent(data)
				if err != nil || e == nil {
					continue
				}
				e.TimeStamp = msg.Timestamp
				dt = playEventToSenderData(e)
			} else if data[1] == "play_start.v5" {
				e, err = krp.parsePlayStartEvent(data)
				if err != nil || e == nil {
					continue
				}
				e.TimeStamp = msg.Timestamp
				dt = playStartEventToSenderData(e)
			} else if data[1] == "play_end.v5" {
				e, err = krp.parsePlayEndEvent(data)
				if err != nil || e == nil {
					continue
				}
				e.TimeStamp = msg.Timestamp
				dt = playEndEventToSenderData(e)
			} else {
				continue
			}
			domains := krp.domains
			for _, v := range domains {
				if v == e.Domain {
					// fmt.Println("parse domain", v, e.Domain)
					datas = append(datas, dt)
				}
			}
		}
	}
	return datas, nil
}

func NewKafkaQosPlayParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(parser.KeyParserName, "")
	ipDb, _ := c.GetStringOr("ipdb", "")
	err := ip17mon.Init(ipDb)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	apmHost, _ := c.GetStringOr("apm_host", "")
	playDomainRetrievePath, _ := c.GetStringOr("play_domain_retrieve_path", "")
	domains, err := getDomains(apmHost, playDomainRetrievePath)
	if err != nil {
		return nil, err
	}
	kafkaQosPlayParser := &KafkaQosPlayParser{
		name:                   name,
		domains:                domains,
		apmHost:                apmHost,
		playDomainRetrievePath: playDomainRetrievePath,
	}
	log.Info("successfully set play domains to %v", domains)
	kafkaQosPlayParser.RefreshDomains()
	return kafkaQosPlayParser, nil
}

func getDomains(apmHost, path string) ([]string, error) {
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
	return domains, nil
}
