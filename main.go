package main

import (
	"flag"
	"log"
	"os"
	"path"
	"runtime"
	"syscall"
	"time"

	"github.com/aerospike/aerospike-client-go"
	daemon "github.com/sevlyar/go-daemon"
)

var (
	spec                      = map[string]interface{}{}
	rootPath    string        = currentDir()
	pidFile     string        = "log/loadgen.pid"
	logFile     string        = "log/loadgen.log"
	configFile  string        = "etc/config.yml"
	logInterval time.Duration = time.Second
	verbose     bool          = false
	signame     string        = ""

	executor *Executor = nil
)

func main() {

	// error
	// var err error

	// parse arguments
	flag.StringVar(&pidFile, "pid", pidFile, "Path to PID file.")
	flag.StringVar(&logFile, "log", logFile, "Path to log file.")
	flag.StringVar(&configFile, "config", configFile, "Path to configuration file.")
	flag.DurationVar(&logInterval, "log-interval", logInterval, "Logging interval in seconds.")
	flag.BoolVar(&verbose, "verbose", verbose, "Verbose logging to stdout.")
	flag.Parse()

	command := ""
	if flag.NArg() == 1 {
		command = flag.Arg(0)
	}

	// daemon signal handlers
	daemon.AddCommand(daemon.StringFlag(&command, "quit"), syscall.SIGQUIT, sigTerm)
	daemon.AddCommand(daemon.StringFlag(&command, "stop"), syscall.SIGTERM, sigTerm)
	daemon.AddCommand(daemon.StringFlag(&command, "reload"), syscall.SIGHUP, sigHup)

	// check files
	pidFile = checkFile(pidFile)
	logFile = checkFile(logFile)
	configFile = checkFile(configFile)

	// daemon context
	context := &daemon.Context{
		PidFileName: pidFile,
		PidFilePerm: 0644,
		LogFileName: logFile,
		LogFilePerm: 0644,
		WorkDir:     "./",
		Umask:       027,
		Args:        []string{},
		Credential:  &syscall.Credential{},
	}

	// check active flags
	if len(daemon.ActiveFlags()) > 0 {
		d, err := context.Search()
		if err != nil {
			log.Fatalln("Unable send signal to the daemon:", err)
		}
		daemon.SendCommands(d)
		return
	}

	switch command {
	case "status":
		cmdStatus(context)
	case "start":
		cmdStart(context)
	}
}

func panicOnError(err error) {
	if err != nil {
		logError("%v", err)
		panic(err)
	}
}

func currentDir() string {
	s, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return s
}

func checkFile(file string) string {

	var err error = nil

	if !path.IsAbs(file) {
		file = path.Join(rootPath, file)
	}

	_, err = os.Stat(file)
	if err != nil {
		if os.IsNotExist(err) {
			dir := path.Dir(file)
			err = os.MkdirAll(dir, 0755)
			if err != nil {
				log.Panic(err)
			}
		} else {
			log.Panic(err)
		}
	}

	return file
}

func checkDir(dir string) string {

	var err error = nil

	if !path.IsAbs(dir) {
		dir = path.Join(rootPath, dir)
	}

	_, err = os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(dir, 0755)
			if err != nil {
				log.Panic(err)
			}
		} else {
			log.Panic(err)
		}
	}

	return dir
}

func sigTerm(sig os.Signal) error {
	os.Exit(0)
	return nil
}

func sigHup(sig os.Signal) error {
	// ex := executor
	// executor = execute()
	// ex.Stop()
	return nil
}

func cmdStatus(context *daemon.Context) {

	if err := os.Stat(context.PidFileName); err != nil {
		if !os.IsExist(err) {
			println("stopped")
			os.Exit(0)
		} else {
			println("error: ", err.Error())
			os.Exit(1)
		}
	}

	proc, err := context.Search()
	if err != nil {
		println("error: ", err.Error())
		os.Exit(1)
	}

	err = proc.Signal(syscall.Signal(0))
	if err == nil {
		println("running")
	} else {
		println("stopped")
	}
	os.Exit(0)
}

func cmdStart(context *daemon.Context) {

	var err error = nil

	// check active flags
	if len(daemon.ActiveFlags()) > 0 {
		d, err := context.Search()
		if err != nil {
			log.Fatalln("Unable send signal to the daemon:", err)
		}
		daemon.SendCommands(d)
		return
	}

	// spawn daemon
	d, err := context.Reborn()
	if err != nil {
		log.Fatalln(err)
	}
	if d != nil {
		return
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
}
