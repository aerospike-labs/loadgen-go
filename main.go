package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"syscall"
	"time"

	daemon "github.com/sevlyar/go-daemon"
)

var (
	spec             = map[string]interface{}{}
	loadId   *string = flag.String("load", "", "Load plan to execute.")
	keyCount *int    = flag.Int("keys", 1000000, "Number of keys to generate.")
)

func panicOnError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	// read flags
	var (
		signal = flag.String("s", "", "send signal to the daemon\n\tquit — graceful shutdown\n\tstop — fast shutdown\n\treload — reloading the configuration file")

		filename    *string        = flag.String("config", "load.yaml", "Load spec filename.")
		logfile     *string        = flag.String("log", "", "Log file.")
		loginterval *time.Duration = flag.Duration("log-interval", time.Second, "Logging interval in seconds.")
		pidfile     *string        = flag.String("pid", "loadgen-go.pid", "pid file.")
	)

	flag.Parse()

	var err error

	if *signal == "" {
		if *logfile == "" {
			log.SetOutput(os.Stdout)
		} else {
			f, err := os.OpenFile(*logfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
			if err != nil {
				log.Fatalf("error opening file: %v", err)
			}
			defer f.Close()
			log.SetOutput(f)
		}
	} else {
		daemon.AddCommand(daemon.StringFlag(signal, "quit"), syscall.SIGQUIT, termHandler)
		daemon.AddCommand(daemon.StringFlag(signal, "stop"), syscall.SIGTERM, termHandler)
		daemon.AddCommand(daemon.StringFlag(signal, "reload"), syscall.SIGHUP, reloadHandler)
		// daemon.AddCommand(daemon.StringFlag(signal, "status"), syscall.SIGINFO, statusHandler)

		log.SetOutput(ioutil.Discard)
		cntxt := &daemon.Context{
			PidFileName: *pidfile,
			PidFilePerm: 0644,
			LogFileName: *logfile,
			LogFilePerm: 0640,
			WorkDir:     "./",
			Umask:       027,
			Args:        os.Args,
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
		log.Println("- - - - - - - - - - - - - - -")
		log.Println("daemon started")
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	// run stats service
	go statsService(*loginterval)

	lp := NewLoadPlan(*filename)

	if *signal != "" {
		go lp.Watch()
		err = daemon.ServeSignals()
		if err != nil {
			log.Println("Error:", err)
		}
		log.Println("daemon terminated")
	} else {
		lp.Watch()
	}
}

func termHandler(sig os.Signal) error {
	log.Println("terminating...")
	return daemon.ErrStop
}
func reloadHandler(sig os.Signal) error {
	log.Println("configuration reloaded")
	return nil
}

func statusHandler(sig os.Signal) error {
	println("Up and running")
	return nil
}
