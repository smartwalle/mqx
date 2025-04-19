package nsqx

import "github.com/nsqio/go-nsq"

const (
	LogLevelDebug   = nsq.LogLevelDebug
	LogLevelInfo    = nsq.LogLevelInfo
	LogLevelWarning = nsq.LogLevelWarning
	LogLevelError   = nsq.LogLevelError
)

type Logger interface {
	Output(calldepth int, s string) error
}
