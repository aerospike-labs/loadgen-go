package main

import (
	"flag"
	"log"
	"runtime"
)

var spec = map[string]interface{}{}

func panicOnError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	// read flags
	var filename *string = flag.String("config", "load.yaml", "Load spec filename.")
	flag.Parse()

	// run stats service
	go statsService()

	lp := NewLoadPlan(*filename)
	lp.Watch()
}
