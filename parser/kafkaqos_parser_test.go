package parser

import (
	"strings"
	"testing"

	"github.com/qiniu/logkit/conf"

	"github.com/stretchr/testify/assert"
)

func TestKafaQosStreamParser(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "krp-2"
	c[KeyParserType] = "kafka_qos_stream"
	ps := NewParserRegistry()
	p, err := ps.NewLogParser(c)
	if err != nil {
		t.Error(err)
	}
	lines := []string{
		`115.171.197.205 stream.v5       1501764520535   1501246013117237        2.2.0.1 rtmp    pili-publish.ruibaokeji.cn      /rbzhibo/p8zyipxqwjv    rbzh1501764006  42.81.55.134    1501764460483   1501764520534   20      0       43      20      43      9       0       43      98925   344693  29`,
		`115.171.197.205 stream.v5       1501764580592   1501246013117237        2.2.0.1 rtmp    pili-publish.ruibaokeji.cn      /rbzhibo/p8zyipxqwjv    rbzh1501764006  42.81.55.134    1501764520535   1501764580592   20      0       43      20      43      10      0       43      98072   371888  32`,
		`222.182.171.16  stream.v5       1501764577838   1F164B4E-4AF8-40D0-A13C-A9DC23B28CCC    git-2017-04-28-09109eb1 rtmp    push.ws.lvdou66.com     /vod/20429117   vod1501764547   61.166.128.47   1501764547838   1501764577838   24      0       46      23      46      24      0       47      75199   956636  0.01`,
		`211.161.240.207 stream.v5       1501764540201   1492085420769677        2.2.0   rtmp    114.55.127.56   /megatest/g14492790s2t1501764539816u3414691i29  mega1501764539  114.55.127.56   1501763828557   1501764540201   4       0       1       4       1       0       0       0       2296    0       69`,
		`a b c d e f g h i j k l m n o p`,
		`abcd efg Warn hijk`,
	}
	dts, err := p.Parse(lines)
	if err != nil {
		t.Error(err)
	}
	expected_result_post := make(map[string]interface{})
	expected_result_post[KEY_SRC_IP] = "172.16.16.191"
	expected_result_post[KEY_TOPIC] = "VIP_VvBVy0tuMPPspm1A_0000000000"
	expected_result_post[KEY_METHOD] = "POST"
	expected_result_post[KEY_CODE] = 200
	expected_result_post[KEY_DURATION] = 46
	expected_result_post[KEY_RESP_LEN] = 101640
	post_line := dts[0]
	for k, v := range expected_result_post {
		if v != post_line[k] {
			t.Errorf("unexpected result get of key:%v, %v not equal %v", k, post_line[k], v)
		}
	}

	expected_result_get := make(map[string]interface{})
	expected_result_get[KEY_SRC_IP] = "192.168.85.32"
	expected_result_get[KEY_TOPIC] = "VIP_XfH2Fd3NRCuZpqyP_0000000000"
	expected_result_get[KEY_METHOD] = "GET"
	expected_result_get[KEY_CODE] = 200
	expected_result_get[KEY_DURATION] = 211
	expected_result_get[KEY_RESP_LEN] = 448238
	get_line := dts[3]
	for k, v := range expected_result_get {
		if v != get_line[k] {
			t.Errorf("unexpected result get of key:%v, %v not equal %v", k, get_line[k], v)
		}
	}
	assert.EqualValues(t, "krp-1", p.Name())
}

func TestKafaQosStreamParseField(t *testing.T) {
	rest_parser := &KafaRestlogParser{}
	log := `[2016-12-05 03:35:20,682] INFO 172.16.16.191 - - [05/Dec/2016:03:35:20 +0000] "POST /topics/VIP_VvBVy0tuMPPspm1A_0000000000 HTTP/1.1" 200 101640  46 (io.confluent.rest-utils.requests)` + "\n"
	fields := strings.Split(log, " ")
	time := rest_parser.ParseLogTime(fields)

	ip := rest_parser.ParseIp(fields)
	if ip == EMPTY_STRING {
		t.Error("failed to parse field ip")
	}
	method := rest_parser.ParseMethod(fields)
	if method == EMPTY_STRING {
		t.Error("failed to parse field method")
	}

	topic := rest_parser.ParseTopic(fields)
	if topic == EMPTY_STRING {
		t.Error("failed to parse field topic")
	}

	code := rest_parser.ParseCode(fields)
	if code == 0 {
		t.Error("failed to parse field code")
	}

	resp_len := rest_parser.ParseRespCL(fields)
	if resp_len == 0 {
		t.Error("failed to parse field resplen")
	}

	duration := rest_parser.ParseDuration(fields)
	if duration == 0 {
		t.Error("failed to parse field duration")
	}

	t.Log(ip)
	t.Log(method)
	t.Log(topic)
	t.Log(code)
	t.Log(resp_len)
	t.Log(duration)
	t.Log(time)
}
