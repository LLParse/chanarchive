package main

import (
	"flag"
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
	flag.Parse()
	fc := node.Flags
	if fc.EtcdEndpoints == "" {
		fmt.Printf("ERROR: Invalid etcd nodes (%s) specified. \n\nView the command line options with `%s node -h` \nOr read the docs online at Github.\n", fc.EtcdEndpoints, os.Args[0])
		fmt.Printf("Flags: \n")
		flag.PrintDefaults()
		os.Exit(1)
	}

	serverNode := node.NewNode(fc)
	stop := make(chan bool)
	ctrlc(stop)
	e := serverNode.Bootstrap()
	if e != nil {
		os.Exit(1)
	}

	if fc.HttpPort != 0 {
		ns := node.NewNodeServer(serverNode, stop)
		go ns.Serve(fc.HttpPort)
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
