package main

import (
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/aerospike/aerospike-client-go/types"
)

var count, totalCount, timeouts, totalTimeouts, errs, totalErrs uint64
var statLog = []*StatsLog{}

var stats = map[string]*OpStat{
	OPGET:      &OpStat{Op: OPGET},
	OPPUT:      &OpStat{Op: OPPUT},
	OPDELETE:   &OpStat{Op: OPDELETE},
	OPSCAN:     &OpStat{Op: OPSCAN},
	OPQUERY:    &OpStat{Op: OPQUERY},
	OPEXEC_UDF: &OpStat{Op: OPEXEC_UDF},
}

type OpStat struct {
	Op       string
	Count    uint64
	Timeouts uint64
	Errs     uint64
}

type StatsLog struct {
	Timestamp time.Time
	OpStat
}

func atomicLog(op string) *StatsLog {
	newStats := &OpStat{Op: op}
	stat := stats[op]
	stats[op] = newStats
	newLog := &StatsLog{
		Timestamp: time.Now(),
		OpStat:    *stat,
	}

	statLog = append(statLog, newLog)
	return newLog
}

func atomicStat(op string, err error) {
	stat := stats[op]

	if err == nil {
		atomic.AddUint64(&stat.Count, 1)
	} else if err != nil {
		t, ok := err.(types.AerospikeError)
		if ok && t.ResultCode() == types.TIMEOUT {
			atomic.AddUint64(&stat.Timeouts, 1)
		} else {
			atomic.AddUint64(&stat.Errs, 1)
		}
	}
}

func statsService(interval time.Duration) {
	logs := make([]*StatsLog, 6)
	logStr := bytes.NewBuffer(nil)
	for {
		select {
		case <-time.After(interval):
			logs[0] = atomicLog(OPGET)
			logs[1] = atomicLog(OPPUT)
			logs[2] = atomicLog(OPDELETE)
			logs[3] = atomicLog(OPSCAN)
			logs[4] = atomicLog(OPQUERY)
			logs[5] = atomicLog(OPEXEC_UDF)

			for _, l := range logs {
				logStr.WriteString(fmt.Sprintf("{%s: count=%d, timeouts=%d, errors=%d} ", l.Op, l.Count, l.Timeouts, l.Errs))
			}
			log.Println(logStr.String())
			logStr.Reset()
		}
	}
}
