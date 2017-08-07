package sender

// import (
// 	"github.com/qiniu/logkit/conf"
// )

// type MultiPandoraSender struct {
// 	Senders map[string]*PandoraSender
// }

// func NewMultiPandoraSender(conf conf.MapConf) (sender Sender, err error) {
// 	instance := MultiPandoraSender{map[string]*PandoraSender{}}
// 	for k, v := range conf {
// 		c := v.(conf.MapConf)
// 		x, err := NewPandoraSender(c)
// 		if err == nil && x != nil {
// 			instance.Senders[k] = NewPandoraSender(c)
// 		}
// 	}
// 	return &instance, nil
// }

// func (s *MultiPandoraSender) Name() string {
// 	return "MultiPandoraSender"
// }

// func (s *MultiPandoraSender) Close() error {
// 	for _, v := range s.Senders {
// 		v.client.Close()
// 	}
// 	return nil
// }

// func (s *MultiPandoraSender) splitData(datas []Data) map[string][]Data {
// 	datasMap := map[string][]Data{}
// 	for _, v := range datas {
// 		if x, ok := v["domain"]; ok {
// 			domain := x.(string)
// 			datasMap[domain] = v
// 		}
// 	}
// 	return datasMap
// }

// func (s *MultiPandoraSender) Send(datas []Data) (se error) {
// 	datasMap := s.splitData(datas)
// 	for k, v := range datasMap {
// 		if sender, ok := s.Senders[k]; ok {
// 			Sender.Send(v)
// 		}
// 	}
// }
