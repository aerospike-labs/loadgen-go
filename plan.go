package main

import (
	"io/ioutil"
	"log"
	"math/rand"
	"regexp"
	"runtime"
	"strconv"
	"strings"
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
	operations     map[string]func()
	loadGenerators []func()

	client *aerospike.Client
}

func NewLoadPlan(filename string) *LoadPlan {
	return &LoadPlan{
		filename:       filename,
		loadGenerators: make([]func(), 100),
		operations:     map[string]func(){},
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
	TIMEOUT  = "Timeout"
	KEYCOUNT = "KeyCount"
	HOSTS    = "Hosts"
	HOST     = "Host"
	PORT     = "Port"
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

	// create the default UDF for test
	createDefaultUDF(lp.client)

	log.Println("Nodes found: ", len(lp.client.GetNodeNames()))
	for _, nodeName := range lp.client.GetNodeNames() {
		log.Println(nodeName)
	}

	// verify plans
	operations := lp.plan[OPERATIONS].([]interface{})

	// create the plans
	for _, opDesc := range operations {
		op := opDesc.(map[interface{}]interface{})
		opId := op[ID].(string)
		lp.operations[opId] = makeOp(lp.client, op)
	}

	// setup load
	// loadPlans := lp.plan[LOAD].([]interface{})
	// var loadPlan map[interface{}]interface{}
	// for _, p := range loadPlans {
	// 	lPlan := p.(map[interface{}]interface{})
	// 	id := lPlan[ID].(string)
	// 	if id == *loadId {
	// 		loadPlan = lPlan
	// 		delete(loadPlan, ID)
	// 		break
	// 	}
	// }

	// if loadPlan == nil {
	// 	log.Fatalf("Load `%s` not found in defined operations.", *loadId)
	// }

	// find the load plan that is requested
	// loadTotal := 0
	// offset := 0
	// for opId, pct := range loadPlan {
	// 	if lp.operations[opId.(string)] == nil {
	// 		log.Fatalf("Plan `%s` not found in defined operations.", opId)
	// 	}

	// 	load := readPercent(pct)
	// 	loadTotal += load
	// 	if loadTotal > 100 {
	// 		log.Fatal("Total load for operations should equal to exactly 100%")
	// 	}

	// 	for i := 0; i < load; i++ {
	// 		lp.loadGenerators[offset] = lp.operations[opId.(string)]
	// 		offset++
	// 	}
	// }

	// if loadTotal != 100 {
	// 	log.Fatal("Total load for operations should equal to exactly 100%")
	// }

	// reset map
	keySet = NewKeySet(1)
}

func readPercent(pct interface{}) int {
	switch pct.(type) {
	case string:

		re := regexp.MustCompile(`(\d{1,3})\%?`)
		values := re.FindStringSubmatch(pct.(string))

		// see if the value is supplied
		if len(values) == 2 && strings.Trim(values[1], " ") != "" {
			if value, err := strconv.Atoi(strings.Trim(values[1], " ")); err == nil && value >= 0 && value <= 100 {
				return value
			}
		}

	case int:
		if pct.(int) >= 0 && pct.(int) <= 100 {
			return pct.(int)
		}
	}
	log.Fatalf("`%v` is not a valid percent value.", pct)
	return 0
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
			rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
			for {
				select {
				case <-hc:
					return
				default:
					lp.loadGenerators[rnd.Intn(100)]()
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
