package vdnqos

import (
	"testing"
	"strings"
	"github.com/stretchr/testify/assert"
)

func TestParsePlayEventData(t *testing.T) {
	string := `42.101.64.107	play.v5	1573275648866	156786086438882	1.1.0.79	http
	pili-live-hdl.rela.me	rela-live/102360744.flv	-	60.195.240.74	15732755
	88668	1573275648866	0	19.69	0	47.00	0	0.00	34.00
	3498	3413	99090	1292630`
	data := strings.Split(string, "\t")
	e, err := ParsePlayEventData(data)
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, "play.v5", e.Tag)
}

func TestParsePlayStartEventData(t *testing.T) {
	string := `210.13.93.225	play_start.v5	1573194380551	101046623	1.1.0.79
	http	153.3.231.121	flv.live-ali.rela.me/live/100335063.flv	-	153.3.23
	1.121	129	122	3000	h264	aac	22228	22256`
	data := strings.Split(string, "\t")
	e, err := ParsePlayStartEventData(data)
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, "play_start.v5", e.Tag)
}

func TestParsePlayEndEventData(t *testing.T) {
	string := `210.13.93.225	play_end.v5	1573194450350	101390544	1.1.0.79
	http	153.3.231.121	flv.live-ali.rela.me/live/103424905.flv	-	153.3.23
	1.121	1573194441875	1573194450350	0	0	0	0	2706
	iPhone10,3	iOS	13.2	com.rela	5.1.0	0.00	0.31	0.96
	0.12	ffmpeg-3.0	-	WIFI	10.47.61.148	153.3.231.121	-
	-	0	0	0	0	0	-	-	-`
	data := strings.Split(string, "\t")
	e, err := ParsePlayEndEventData(data)
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, "play_end.v5", e.Tag)
}

func TestParsePlayStartOpenEventData(t *testing.T) {
	string := `171.81.255.128	play_start_op.v5	1573117309229	106252712	1.1.0.79
	http	pili-live-hdl.rela.me	rela-live/104153993.flv	-	-
	iPhone10,1	iOS	11.2.6	com.rela	5.0.7`
	data := strings.Split(string, "\t")
	e, err := ParsePlayStartOpenEventData(data)
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, "play_start_op.v5", e.Tag)
}

func TestParsePlayEndOpenEventData(t *testing.T) {
	string := `210.13.93.225	play_end_op.v5	1573117195295	104153993	1.1.0.79
	http	218.98.28.151	rela-live/106559880.flv	-	218.98.28.151	OPPO
	Android	6.0.1	com.thel	5.1.0	30325	0	-	aac	167
	1	28	43	51828	717698	0	0`
	data := strings.Split(string, "\t")
	e, err := ParsePlayEndOpenEventData(data)
	if err != nil {
		t.Error(err)
	}
	assert.EqualValues(t, "play_end_op.v5", e.Tag)
}
