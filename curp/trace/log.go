package trace

import (
	"fmt"
	"log"
	"time"
)

const debug1 = 0

type logTopic string

const (
	Client   logTopic = "CLNT"
	Commit   logTopic = "CMIT"
	Error    logTopic = "ERRO"
	Info     logTopic = "INFO"
	Leader   logTopic = "LEAD"
	Vote     logTopic = "VOTE"
	Follower logTopic = "FOLW"
	Fast     logTopic = "FAST"
	Slow     logTopic = "SLOW"
	Log      logTopic = "LOG1"
	Term     logTopic = "TERM"
	Warn     logTopic = "WARN"
)

var debugStart time.Time

func init() {
	debugStart = time.Now()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Trace(topic logTopic, id int, format string, a ...interface{}) {
	if debug1 >= 1 {
		//time := time.Since(debugStart).Microseconds()
		//time /= 100
		now := time.Now()
		formattedTime := now.Format("05.000")
		prefix := fmt.Sprintf("[%s] %v [%d] ", string(formattedTime), string(topic), id)
		format = prefix + format
		log.Printf(format, a...)
	}
}
