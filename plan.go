package main

import (
	"io/ioutil"
	"log"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go"
	"gopkg.in/fsnotify.v1"
	"gopkg.in/yaml.v2"
)

type LoadPlan struct {
	mutex    sync.RWMutex
	seeds    []*aerospike.Host
	plan     map[interface{}]interface{}
	filename string

	chanHalt       chan bool
	loadGenerators []func()

	client *aerospike.Client
}

func NewLoadPlan(filename string) *LoadPlan {
	return &LoadPlan{
		filename:       filename,
		loadGenerators: make([]func(), 100),
		chanHalt:       make(chan bool),
	}
}

func (lp *LoadPlan) Watch() {
	watcher, err := fsnotify.NewWatcher()
	panicOnError(err)
	defer lp.client.Close()
	defer watcher.Close()

	done := make(chan bool)

	// read plan file
	lp.initPlan()

	go func() {
		for {
			select {
			case event := <-watcher.Events:
				log.Println("event:", event)
				if event.Op&fsnotify.Write == fsnotify.Write {
					log.Println("modified file: ", event.Name)
					lp.stopPlan()
					lp.initPlan()
				}
			case err := <-watcher.Errors:
				log.Println("error:", err)
			}
		}
	}()

	err = watcher.Add(lp.filename)
	if err != nil {
		log.Fatal(err)
	}
	<-done
}

func (lp *LoadPlan) initPlan() {
	log.Println("initialzing plan...")
	lp.readPlanFile()
	lp.interpretPlan()
	go lp.executePlan()
}

func (lp *LoadPlan) readPlanFile() {
	bytes, err := ioutil.ReadFile(lp.filename)
	panicOnError(err)

	lp.mutex.RLock()
	defer lp.mutex.RUnlock()

	lp.plan = make(map[interface{}]interface{})
	err = yaml.Unmarshal(bytes, &lp.plan)
	panicOnError(err)
}

const (
	TIMEOUT  = "timeout"
	KEYCOUNT = "key_count"
	HOSTS    = "hosts"
	HOST     = "host"
	PORT     = "port"
)

func (lp *LoadPlan) interpretPlan() {
	if lp.client != nil {
		lp.client.Close()
	}

	policy := aerospike.NewClientPolicy()
	if lp.plan[TIMEOUT] != nil {
		policy.Timeout = time.Duration(lp.plan[TIMEOUT].(int)) * time.Millisecond
	}

	lp.seeds = []*aerospike.Host{}

	for _, h := range lp.plan[HOSTS].([]interface{}) {
		hostInfo := h.(map[interface{}]interface{})
		lp.seeds = append(lp.seeds, aerospike.NewHost(hostInfo[HOST].(string), hostInfo[PORT].(int)))
	}

	var err error
	lp.client, err = aerospike.NewClientWithPolicyAndHost(policy, lp.seeds...)
	panicOnError(err)

	log.Println("Nodes found: ", len(lp.client.GetNodeNames()))
	for _, nodeName := range lp.client.GetNodeNames() {
		log.Println(nodeName)
	}

	// verify plans
	operations := lp.plan[OPERATIONS].([]interface{})
	loadTotal := 0
	for _, op := range operations {
		loadTotal += op.(map[interface{}]interface{})[LOAD].(int)
	}
	if loadTotal != 100 {
		log.Fatal("Total load for operations should equal to exactly 100%")
	}

	// create the plans
	offset := 0
	for _, opDesc := range operations {
		op := opDesc.(map[interface{}]interface{})
		load := op[LOAD].(int)
		for i := 0; i < load; i++ {
			lp.loadGenerators[offset] = makeOp(lp.client, op)
			offset++
		}
	}

	// reset map
	keyCount := readOption(lp.plan, KEYCOUNT, 1000000).(int)
	keySet = NewKeySet(keyCount)
}

func (lp *LoadPlan) stopPlan() {
	lp.chanHalt <- true
	<-lp.chanHalt
}

func (lp *LoadPlan) executePlan() {
	// run load generators
	haltChannels := []chan bool{}

	for i := 0; i < runtime.NumCPU()*2; i++ {
		hChan := make(chan bool)
		haltChannels = append(haltChannels, hChan)

		go func(hc chan bool) {
			for {
				select {
				case <-hc:
					return
				default:
					lp.loadGenerators[rand.Intn(100)]()
				}
			}
		}(hChan)
	}

	<-lp.chanHalt
	log.Println("Stopped generating load...")
	for _, hc := range haltChannels {
		hc <- true
	}
	lp.chanHalt <- true
}
