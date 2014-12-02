package main

import (
	as "github.com/aerospike/aerospike-client-go"
	"sync/atomic"
)

type ReadOp struct {
	key  *as.Key
	bins []string
}

type WriteOp struct {
	key  *as.Key
	bins []as.Bin
}

// type DeleteOp struct {
// 	key *as.Key
// }

// type QueryOp struct {
// 	Namespace string
// 	SetName   string
// }

// type ScanOp struct {
// 	Namespace string
// 	SetName   string
// }

type OpChannels struct {
	Reads  chan *ReadOp
	Writes chan *WriteOp
	// Deletes chan *DeleteOp
	// Queries chan *QueryOp
	// Scans   chan *ScanOp
}

type LoadGenerator struct {
	Keys     KeyGenerator
	Records  RecordGenerator
	Client   *as.Client
	Model    *LoadModel
	Data     *DataModel
	Done     chan int
	Channels OpChannels
	State    uint32
}

func NewLoadGenerator(model *LoadModel, keys KeyGenerator, records RecordGenerator, client *as.Client) *LoadGenerator {
	g := &LoadGenerator{
		Model:   model,
		Keys:    keys,
		Records: records,
		Client:  client,
		Done:    make(chan int),
		State:   0,
		Channels: OpChannels{
			Reads:  make(chan *ReadOp, model.Reads),
			Writes: make(chan *WriteOp, model.Writes),
			// Deletes: make(chan *DeleteOp, model.Deletes),
			// Queries: make(chan *QueryOp, model.Queries),
			// Scans:   make(chan *ScanOp, model.Scans),
		},
	}
	return g
}

func (g *LoadGenerator) generateReads() {
	for {
		s := atomic.LoadUint32(&g.State)
		if s == 1 {
			return
		}

		k := g.Keys.GenerateKey()
		_, err := g.Client.Get(nil, k)
		if err != nil {
			logError("%v", err.Error())
		}
	}
}

func (g *LoadGenerator) generateWrites() {
	for {
		s := atomic.LoadUint32(&g.State)
		if s == 1 {
			return
		}

		k := g.Keys.GenerateKey()
		r := g.Records.GenerateRecord()
		err := g.Client.PutBins(nil, k, r...)
		if err != nil {
			logError("%v", err.Error())
		}
	}
}

func (g *LoadGenerator) Start() {
	var i int64

	// reads
	for i = 0; i < g.Model.Reads; i++ {
		go g.generateReads()
	}

	// writes
	for i = 0; i < g.Model.Writes; i++ {
		go g.generateWrites()
	}
}

func (g *LoadGenerator) Stop() {
	atomic.StoreUint32(&g.State, uint32(1))
}

func (g *LoadGenerator) Wait() {
	<-g.Done
}

func (g *LoadGenerator) Terminate() {
	g.Done <- 1
}
