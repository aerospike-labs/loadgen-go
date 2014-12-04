package main

import (
	"flag"
	"io"
	"log"
	"os"
	// "os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/aerospike/aerospike-client-go"
	daemon "github.com/sevlyar/go-daemon"
)

var (
	spec                      = map[string]interface{}{}
	addr        string        = "0.0.0.0"
	port        int           = 3000
	pidFile     string        = "loadgen.pid"
	logFile     string        = "loadgen.log"
	modelsFile  string        = "models.yml"
	loadId      string        = "default"
	dataId      string        = "default"
	logInterval time.Duration = time.Second
	verbose     bool          = false
	signame     string        = ""

	executor *Executor         = nil
	client   *aerospike.Client = nil
)

func main() {

	// error
	var err error

	// parse arguments
	flag.StringVar(&pidFile, "pid", pidFile, "Path to PID file.")
	flag.StringVar(&logFile, "log", logFile, "Path to log file.")
	flag.StringVar(&addr, "addr", addr, "Address to a machine in the cluster.")
	flag.IntVar(&port, "port", port, "Port to a machine in the cluster.")
	flag.StringVar(&modelsFile, "models", modelsFile, "Path to models specification file.")
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

	// load models
	models := NewModels()
	err := models.Load(modelsFile)
	panicOnError(err)

	if client != nil {
		client.Close()
	}

	hosts := make([]*aerospike.Host, len(models.Hosts))
	for i, h := range models.Hosts {
		hosts[i] = aerospike.NewHost(h.Addr, h.Port)
	}

	client, err = aerospike.NewClientWithPolicyAndHost(nil, hosts...)
	panicOnError(err)

	// create generators
	keys := NewPooledKeyGenerator(models.LoadModels[0], models.DataModels[0])
	recs := NewPooledRecordGenerator(models.LoadModels[0], models.DataModels[0])

	// new executor
	exec := NewExecutor(client, models.LoadModels[0], keys, recs)

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
