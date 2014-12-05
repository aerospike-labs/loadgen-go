package main

import (
	"github.com/aerospike/aerospike-client-go"
)

type Executor struct {
	Client  *aerospike.Client
	Load    *LoadModel
	Keys    KeyGenerator
	Records RecordGenerator
	halt    chan bool
}

func NewExecutor(client *aerospike.Client, load *LoadModel, keys KeyGenerator, records RecordGenerator) *Executor {
	return &Executor{
		Client:  client,
		Load:    load,
		Keys:    keys,
		Records: records,
		halt:    make(chan bool),
	}
}

func (e *Executor) Stop() {
	e.halt <- true
	<-e.halt
	e.Client.Close()
	e.Client = nil
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

	// run load generators
	haltChannels := []chan bool{}

	var i int64 = 0
	var o int64 = 0

	if e.Load.Reads > 0 {
		readOp := ReadGenerator(e.Client, e.Keys)
		for i = 0; i < e.Load.Reads; i++ {
			halt := make(chan bool)
			haltChannels = append(haltChannels, halt)
			go executeOp(halt, readOp)
		}
		o += i
	}

	if e.Load.Writes > 0 {
		writeOp := WriteGenerator(e.Client, e.Keys, e.Records)
		for i = 0; i < e.Load.Writes; i++ {
			halt := make(chan bool)
			haltChannels = append(haltChannels, halt)
			go executeOp(halt, writeOp)
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
