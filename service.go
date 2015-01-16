package main

import (
	. "github.com/aerospike-labs/minion/service"

	"log"
	// "os"
	"runtime"
	"syscall"

	"github.com/aerospike/aerospike-client-go"
	daemon "github.com/sevlyar/go-daemon"
)

type LoadgenService struct {
	context *daemon.Context
}

func (svc *LoadgenService) Install(params map[string]interface{}) error {
	return nil
}

func (svc *LoadgenService) Remove() error {
	return nil
}

func (svc *LoadgenService) Status() (Status, error) {

	proc, err := svc.context.Search()
	if err != nil {
		return Stopped, err
	}

	err = proc.Signal(syscall.Signal(0))
	if err == nil {
		return Running, nil
	} else {
		return Stopped, nil
	}
}

func (svc *LoadgenService) Start() error {

	var err error = nil
	context := svc.context

	// check active flags
	if len(daemon.ActiveFlags()) > 0 {
		d, err := context.Search()
		if err != nil {
			log.Fatalln("Unable send signal to the daemon:", err)
		}
		daemon.SendCommands(d)
		return nil
	}

	// spawn daemon
	d, err := context.Reborn()
	if err != nil {
		log.Fatalln(err)
	}
	if d != nil {
		return nil
	}
	defer context.Release()

	// utlize full cores
	runtime.GOMAXPROCS(runtime.NumCPU())

	// services
	go statsService(logInterval)

	// execute the current model
	logInfo("Loading Executor")

	// load config
	config := NewConfig()
	err = config.Load(configFile)
	panicOnError(err)

	// iterate over hosts
	hosts := make([]*aerospike.Host, len(config.Hosts))
	for i, h := range config.Hosts {
		hosts[i] = aerospike.NewHost(h.Addr, h.Port)
		logInfo("Adding host (%s:%v)", h.Addr, h.Port)
	}

	// create client
	client, err := aerospike.NewClientWithPolicyAndHost(nil, hosts...)
	if err != nil {
		logError("Not able to connect to cluster: %s", err.Error())
		panicOnError(err)
	}

	var loadModel *LoadModel = &config.LoadModel
	var dataModel *DataModel = &config.DataModel

	// generate keys
	keys := NewPooledKeyGenerator(dataModel, loadModel.Keys)
	keys.generate()

	// generate record permutations
	recs := NewPooledRecordGenerator(dataModel, 100)
	recs.generate()

	// new executor
	exec := NewExecutor(client, loadModel, keys, recs)

	// run
	logInfo("Running Executor")
	go exec.Run()

	// serve signals
	err = daemon.ServeSignals()
	panicOnError(err)

	// exit handled by signal handlers
	halt := make(chan bool)
	<-halt

	return nil
}

func (svc *LoadgenService) Stop() error {
	context := svc.context

	// check active flags
	if len(daemon.ActiveFlags()) > 0 {
		d, err := context.Search()
		if err != nil {
			log.Fatalln("Unable send signal to the daemon:", err)
		}
		daemon.SendCommands(d)
		return nil
	}
	return nil
}

func (svc *LoadgenService) Stats() (map[string]interface{}, error) {
	stats := map[string]interface{}{}
	return stats, nil
}
