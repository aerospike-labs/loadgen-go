package main

import (
	"flag"
	// "io"
	"log"
	"os"
	// "runtime"
	"path"
	"syscall"
	"time"

	// "github.com/aerospike/aerospike-client-go"
	. "github.com/aerospike-labs/minion/service"
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
	daemon.AddCommand(daemon.StringFlag(&command, "quit"), syscall.SIGQUIT, signalTerm)
	daemon.AddCommand(daemon.StringFlag(&command, "stop"), syscall.SIGTERM, signalTerm)
	daemon.AddCommand(daemon.StringFlag(&command, "reload"), syscall.SIGHUP, signalHup)

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

	// Run service
	Run(&LoadgenService{context})
}

func panicOnError(err error) {
	if err != nil {
		logError("%v", err)
		panic(err)
	}
}

func signalTerm(sig os.Signal) error {
	os.Exit(0)
	return nil
}

func signalHup(sig os.Signal) error {
	// ex := executor
	// executor = execute()
	// ex.Stop()
	return nil
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
