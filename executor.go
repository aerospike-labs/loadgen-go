package main

import (
	"math/rand"
	"runtime"
	"time"

	"github.com/aerospike/aerospike-client-go"
)

type Executor struct {
	Client     *aerospike.Client
	Load       *LoadModel
	Keys       KeyGenerator
	Records    RecordGenerator
	Operations []func()
	halt       chan bool
}

func NewExecutor(client *aerospike.Client, load *LoadModel, keys KeyGenerator, records RecordGenerator) *Executor {
	n := load.Reads + load.Writes
	return &Executor{
		Client:     client,
		Load:       load,
		Keys:       keys,
		Records:    records,
		Operations: make([]func(), n),
		halt:       make(chan bool),
	}
}

func (e *Executor) initialize() int64 {

	var i int64 = 0
	var o int64 = 0

	if e.Load.Reads > 0 {
		for i = 0; i < e.Load.Reads; i++ {
			e.Operations[o+i] = ReadGenerator(e.Client, e.Keys)
		}
		o += i
	}

	if e.Load.Writes > 0 {
		for i = 0; i < e.Load.Writes; i++ {
			e.Operations[o+i] = WriteGenerator(e.Client, e.Keys, e.Records)
		}
		o += i
	}

	return o
}

func (e *Executor) Stop() {
	e.halt <- true
	<-e.halt
	logInfo("Executor stopped.")
}

func (e *Executor) Run() {

	nops := e.initialize()
	ncpu := int64(runtime.NumCPU())
	nthr := nops * ncpu

	// run load generators
	haltChannels := []chan bool{}

	var i int64
	for i = 0; i < nthr; i++ {
		hChan := make(chan bool)
		haltChannels = append(haltChannels, hChan)

		go func(halt chan bool) {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for {
				select {
				case <-halt:
					return
				default:
					e.Operations[r.Intn(len(e.Operations))]()
				}
			}
		}(hChan)
	}

	<-e.halt
	logInfo("Executor stopping...")
	for _, hc := range haltChannels {
		hc <- true
	}
	e.halt <- true
}
