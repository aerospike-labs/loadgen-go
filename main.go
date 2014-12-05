package main

import (
	"flag"
	"io"
	"log"
	"os"
	"runtime"
	"syscall"
	"time"

	"github.com/aerospike/aerospike-client-go"
	daemon "github.com/sevlyar/go-daemon"
)

var (
	spec                      = map[string]interface{}{}
	pidFile     string        = "loadgen.pid"
	logFile     string        = "loadgen.log"
	configFile  string        = "config.yml"
	loadId      string        = "default"
	dataId      string        = "default"
	logInterval time.Duration = time.Second
	verbose     bool          = false
	signame     string        = ""

	executor *Executor = nil
)

func main() {

	// error
	var err error

	// parse arguments
	flag.StringVar(&pidFile, "pid", pidFile, "Path to PID file.")
	flag.StringVar(&logFile, "log", logFile, "Path to log file.")
	flag.StringVar(&configFile, "config", configFile, "Path to configuration file.")
	flag.StringVar(&loadId, "load", loadId, "The identifier of the load model to use.")
	flag.StringVar(&dataId, "data", dataId, "The identifier of the data model to use.")
	flag.DurationVar(&logInterval, "log-interval", logInterval, "Logging interval in seconds.")
	flag.BoolVar(&verbose, "verbose", verbose, "Verbose logging to stdout.")

	flag.StringVar(&signame, "signal", signame, `send signal to the daemon
		quit — graceful shutdown
		stop — fast shutdown
		reload — reloading the configuration file`)

	flag.Parse()

	// daemon signal handlers
	daemon.AddCommand(daemon.StringFlag(&signame, "quit"), syscall.SIGQUIT, signalTerm)
	daemon.AddCommand(daemon.StringFlag(&signame, "stop"), syscall.SIGTERM, signalTerm)
	daemon.AddCommand(daemon.StringFlag(&signame, "reload"), syscall.SIGHUP, signalHup)

	// daemon context

	cntxt := &daemon.Context{
		PidFileName: pidFile,
		PidFilePerm: 0644,
		LogFileName: logFile,
		LogFilePerm: 0644,
		WorkDir:     "./",
		Umask:       027,
		Args:        []string{},
	}

	if len(daemon.ActiveFlags()) > 0 {
		d, err := cntxt.Search()
		if err != nil {
			log.Fatalln("Unable send signal to the daemon:", err)
		}
		daemon.SendCommands(d)
		return
	}

	d, err := cntxt.Reborn()
	if err != nil {
		log.Fatalln(err)
	}
	if d != nil {
		return
	}
	defer cntxt.Release()

	// setup logger
	if logFile == "" {
		log.SetOutput(os.Stdout)
	} else {
		f, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("error opening file: %v", err)
		}
		defer f.Close()

		if verbose {
			log.SetOutput(io.MultiWriter(os.Stdout, f))
		} else {
			log.SetOutput(f)
		}
	}

	// utlize full cores
	runtime.GOMAXPROCS(runtime.NumCPU())

	// services
	go statsService(logInterval)

	// execute the current model
	executor = execute()

	err = daemon.ServeSignals()
	panicOnError(err)

	// exit handled by signal handlers
	halt := make(chan bool)
	<-halt
}

func execute() *Executor {

	logInfo("Loading Executor")

	var err error = nil

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

	var loadModel *LoadModel
	var dataModel *DataModel

	for _, m := range config.LoadModels {
		if m.Id == loadId {
			loadModel = m
		}
	}

	for _, m := range config.DataModels {
		if m.Id == dataId {
			dataModel = m
		}
	}

	// create generators
	keys := NewPooledKeyGenerator(loadModel, dataModel)
	recs := NewPooledRecordGenerator(loadModel, dataModel)

	// new executor
	exec := NewExecutor(client, loadModel, keys, recs)

	// run
	logInfo("Running Executor")
	go exec.Run()

	return exec
}

func panicOnError(err error) {
	if err != nil {
		logError("%v", err)
		panic(err)
	}
}

func signalTerm(sig os.Signal) error {
	logInfo("Signal Received %v", sig)
	executor.Stop()
	os.Exit(0)
	return nil
}

func signalHup(sig os.Signal) error {
	logInfo("Signal Received %v", sig)
	ex := executor
	executor = execute()
	ex.Stop()
	return nil
}
