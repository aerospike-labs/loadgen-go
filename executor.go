package main

import (
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

func executeOp(halt chan bool, op func()) {
	for {
		select {
		case <-halt:
			return
		default:
			op()
		}
	}
}

func (e *Executor) Run() {

	// nops := e.initialize()

	// run load generators
	haltChannels := []chan bool{}

	var i int64 = 0
	var o int64 = 0

	if e.Load.Reads > 0 {
		for i = 0; i < e.Load.Reads; i++ {
			op := ReadGenerator(e.Client, e.Keys)
			halt := make(chan bool)
			haltChannels = append(haltChannels, halt)
			go executeOp(halt, op)
		}
		o += i
	}

	if e.Load.Writes > 0 {
		for i = 0; i < e.Load.Writes; i++ {
			op := WriteGenerator(e.Client, e.Keys, e.Records)
			halt := make(chan bool)
			haltChannels = append(haltChannels, halt)
			go executeOp(halt, op)
		}
		o += i
	}

	<-e.halt
	logInfo("Executor stopping...")
	for _, hc := range haltChannels {
		hc <- true
	}
	e.halt <- true
}
