package main

import (
	"fmt"
	"github.com/llparse/streamingchan/api"
	"github.com/llparse/streamingchan/node"
	"github.com/llparse/streamingchan/version"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"time"
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	runtime.GOMAXPROCS(runtime.NumCPU())
	fmt.Print("\n")
	fmt.Print(":: StreamingChan - 4Chan Streaming API :: \n")
	fmt.Print("\n")

	fmt.Printf("Version - %s\n", version.GitHash)
	fmt.Printf("Build Date - %s\n", version.BuildDate)

	fmt.Print("\n")
	for _, arg := range os.Args {
		switch arg {
		case "-node":
			donode()
			break
		case "-api":
			doapi()
			break
		}
	}
	dohelp()
}

func dohelp() {
	fmt.Printf("Help:\n")
	fmt.Printf("Run `%s node` to start a node.\n", os.Args[0])
	fmt.Printf("Run `%s api` to start a web endpoint.\n", os.Args[0])
	os.Exit(1)
}

func ctrlc(stop chan<- bool) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		forceExit := false
		for _ = range c {
			if forceExit {
				os.Exit(2)
			} else {
				go func() {
					stop <- true
				}()
				forceExit = true
			}
		}
	}()
}

func donode() {
	stop := make(chan bool)
	ctrlc(stop)
	serverNode := node.NewNode(stop)
	if e := serverNode.Bootstrap(); e != nil {
		os.Exit(1)
	}
	<-stop
	serverNode.CleanShutdown()
	os.Exit(0)
}

func doapi() {
	stop := make(chan bool)
	ctrlc(stop)
	apiNode := api.NewApiServer(stop)
	go func() {
		apiNode.Serve()
		stop <- true
	}()
	<-stop
	os.Exit(0)
}
