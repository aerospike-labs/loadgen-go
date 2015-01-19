package main

import (
	. "github.com/aerospike-labs/minion/service"

	"bytes"
	"os"
	"os/exec"
	"strings"
)

const (
	LOADGEN_REPO = "github.com/aerospike-labs/loadgen-go"
)

var (
	svcPath = os.Getenv("SERVICE_PATH")
)

type LoadgenService struct{}

func (svc *LoadgenService) Install(params map[string]interface{}) error {

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var cmd *exec.Cmd
	var err error

	cmd = exec.Command("go", "get", "-u", LOADGEN_REPO)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err != nil {
		return err
	}

	cmd = exec.Command("go", "install", LOADGEN_REPO)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err != nil {
		return err
	}

	return nil
}

func (svc *LoadgenService) Remove() error {
	os.RemoveAll(svcPath)
	return nil
}

func (svc *LoadgenService) Status() (Status, error) {

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var cmd *exec.Cmd
	var err error

	cmd = exec.Command("bin/loadgen-go", "status")
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err != nil {
		return Stopped, err
	}

	out := stdout.String()
	if strings.Contains(out, "running") {
		return Running, nil
	}

	return Stopped, nil
}

func (svc *LoadgenService) Start() error {

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var cmd *exec.Cmd
	var err error

	cmd = exec.Command("bin/loadgen-go", "start")
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err != nil {
		return err
	}

	return nil
}

func (svc *LoadgenService) Stop() error {

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	var cmd *exec.Cmd
	var err error

	cmd = exec.Command("bin/loadgen-go", "stop")
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err != nil {
		return err
	}

	return nil
}

func (svc *LoadgenService) Stats() (map[string]interface{}, error) {
	stats := map[string]interface{}{}
	return stats, nil
}

// Main - should call service.Run, to run the service,
// and process the commands and arguments.
func main() {
	Run(&LoadgenService{})
}
