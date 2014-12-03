package main

import (
	"flag"
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
	addr        string        = "0.0.0.0"
	port        int           = 3000
	pidFile     string        = "loadgen.pid"
	logFile     string        = "loadgen.log"
	modelsFile  string        = "models.yml"
	loadId      string        = "default"
	dataId      string        = "default"
	logInterval time.Duration = time.Second
)

func main() {

	// parse arguments
	flag.StringVar(&pidFile, "pid", pidFile, "Path to PID file.")
	flag.StringVar(&logFile, "log", logFile, "Path to log file.")
	flag.StringVar(&addr, "addr", addr, "Address to a machine in the cluster.")
	flag.IntVar(&port, "port", port, "Port to a machine in the cluster.")
	flag.StringVar(&modelsFile, "models", modelsFile, "Path to models specification file.")
	flag.StringVar(&loadId, "load", loadId, "The identifier of the load model to use.")
	flag.StringVar(&dataId, "data", dataId, "The identifier of the data model to use.")
	flag.DurationVar(&logInterval, "log-interval", logInterval, "Logging interval in seconds.")
	flag.Parse()

	// signal handlers
	daemon.SetSigHandler(termHandler, syscall.SIGTERM)
	daemon.SetSigHandler(termHandler, syscall.SIGQUIT)
	daemon.SetSigHandler(reloadHandler, syscall.SIGHUP)

	// setup logger
	if logFile == "" {
		log.SetOutput(os.Stdout)
	} else {
		f, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("error opening file: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	// utlize full cores
	runtime.GOMAXPROCS(runtime.NumCPU())

	// load models
	models := NewModels()
	models.Load(modelsFile)

	// Aerospike Client
	client, err := aerospike.NewClient(addr, port)
	panicOnError(err)

	// set up key and record generators
	keys := NewPooledKeyGenerator(models.LoadModels[0], models.DataModels[0])
	recs := NewPooledRecordGenerator(models.LoadModels[0], models.DataModels[0])
	exec := NewExecutor(client, models.LoadModels[0], keys, recs)

	exec.Run()
	// defer load.Stop()

	// load.Wait()

}

func panicOnError(err error) {
	if err != nil {
		logError("%v", err)
	}
}

func termHandler(sig os.Signal) error {
	logInfo("terminating...")
	return daemon.ErrStop
}
func reloadHandler(sig os.Signal) error {
	logInfo("configuration reloaded")
	return nil
}

func statusHandler(sig os.Signal) error {
	logInfo("Up and running")
	return nil
}
