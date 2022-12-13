package raft

import (
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	LEVEL_DEBUG int = iota
	LEVEL_INFO
	LEVEL_NOTICE
	LEVEL_WARNING
	LEVEL_ERROR
)

// Debugging
const (
	LogLevel = LEVEL_INFO
)

const debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if debug {
		log.Printf(format, a...)
	}
	return
}

func Debug(format string, v ...interface{}) {
	WriteLog(LEVEL_DEBUG, format, v...)
}

func Info(format string, v ...interface{}) {
	WriteLog(LEVEL_INFO, format, v...)
}

func Notice(format string, v ...interface{}) {
	WriteLog(LEVEL_NOTICE, format, v...)
}

func Warning(format string, v ...interface{}) {
	WriteLog(LEVEL_WARNING, format, v...)
}

func Error(format string, v ...interface{}) {
	WriteLog(LEVEL_ERROR, format, v...)
}

func WriteLog(level int, format string, v ...interface{}) {
	if level < LogLevel {
		return
	}

	var strLevel string
	switch level {
	case LEVEL_DEBUG:
		strLevel = "DEBUG"
	case LEVEL_INFO:
		strLevel = "INFO"
	case LEVEL_NOTICE:
		strLevel = "NOTICE"
	case LEVEL_WARNING:
		strLevel = "WARNING"
	case LEVEL_ERROR:
		strLevel = "ERROR"
	default:
		strLevel = "DEBUG"
	}

	var goroutineID int64
	if level <= LEVEL_DEBUG {
		goroutineID = getGoroutineID()
	}
	log.Printf("%v %s %s", goroutineID, strLevel, fmt.Sprintf(format, v...))
}

// Waits for a random time between two durations and sends the current time on
// the returned channel.
func afterBetween(min time.Duration, max time.Duration) <-chan time.Time {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	d, delta := min, (max - min)
	if delta > 0 {
		d += time.Duration(rand.Int63n(int64(delta)))
	}
	return time.After(d)
}

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

func getGoroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	stk := strings.TrimPrefix(string(buf[:n]), "goroutine")
	idField := strings.Fields(stk)[0]
	id, _ := strconv.ParseInt(idField, 10, 64)
	return id
}
