package main

import (
	"log"
)

func logDebug(format string, a ...interface{}) {
	log.Printf("debug: "+format, a)
}

func logInfo(format string, a ...interface{}) {
	log.Printf("info: "+format, a)
}

func logError(format string, a ...interface{}) {
	log.Printf("error: "+format, a)
}

func logWarn(format string, a ...interface{}) {
	log.Printf("warn: "+format, a)
}
