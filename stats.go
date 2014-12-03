package main

import (
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/aerospike/aerospike-client-go/types"
)

var (
	CURRENT_STATS Stats = Stats{}
)

type Stat struct {
	Count    uint64
	Timeouts uint64
	Errors   uint64
}

type Stats struct {
	Reads  Stat
	Writes Stat
}

func statUpdate(s *Stat, err error) {
	if err == nil {
		statSuccess(s)
	} else {
		t, ok := err.(types.AerospikeError)
		if ok && t.ResultCode() == types.TIMEOUT {
			statTimeout(s)
		} else {
			statError(s)
		}
	}
}

func statSuccess(s *Stat) {
	atomic.AddUint64(&s.Count, 1)
}

func statTimeout(s *Stat) {
	atomic.AddUint64(&s.Timeouts, 1)
}

func statError(s *Stat) {
	atomic.AddUint64(&s.Errors, 1)
}

func statLog(n string, s *Stat, p *Stat) string {

	sc := atomic.LoadUint64(&s.Count)
	st := atomic.LoadUint64(&s.Timeouts)
	se := atomic.LoadUint64(&s.Errors)

	pc := p.Count
	pt := p.Timeouts
	pe := p.Errors

	dc := sc - pc
	dt := st - pt
	de := se - pe

	p.Count = sc
	p.Timeouts = st
	p.Errors = se

	return fmt.Sprintf("{%s: count=%d/%d, timeouts=%d/%d, errors=%d/%d} ", n, dc, sc, dt, st, de, se)
}

func statsService(interval time.Duration) {

	p := Stats{}
	b := bytes.NewBuffer(nil)

	for {
		select {
		case <-time.After(interval):

			b.WriteString(statLog("reads", &CURRENT_STATS.Reads, &p.Reads))
			b.WriteString(statLog("writes", &CURRENT_STATS.Writes, &p.Writes))

			log.Println(b.String())
			b.Reset()
		}
	}
}
