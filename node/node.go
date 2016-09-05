package node

import (
	"encoding/json"
  "golang.org/x/net/context"
  etcd "github.com/coreos/etcd/client"
	"github.com/llparse/streamingchan/fourchan"
	"github.com/satori/go.uuid"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
	"flag"
	"fmt"
)

type Node struct {
	Id               string
	Stats            *NodeStats
	Boards           *fourchan.Boards
	Storage          *fourchan.Storage
	Config           *NodeConfig
	Keys             etcd.KeysAPI
	LMDelay          time.Duration
	CBoard           chan *fourchan.Board
	CThread          chan *fourchan.ThreadInfo
	CPost            chan *fourchan.Post
	CFile            chan *fourchan.File
	BoardStop        []chan bool
	Shutdown         bool
	Closed           bool
	OwnedBoards      []string
	LastNodeIdx      int
	NodeCount        int
	DivideMutex      sync.Mutex
	Info             *NodeInfo
	stop             chan<- bool
}

type NodeInfo struct {
	Id        string `json:"id"`
	Hostname  string `json:"hostname"`
}

type NodeConfig struct {
	EtcdEndpoints []string
	CassEndpoints []string
	CassKeyspace  string
	CassNumConns  int
	Hostname      string
	BindIp        string
	CmdLine       string
	OnlyBoards    []string
	ExcludeBoards []string
	ClusterName   string
	ThreadWorkers int
	HttpPort      int
	NoFiles       bool
	Verbose       bool
}

const (
	nodeLockTTL = 30 * time.Second
)

func (n *Node) Close() {
	n.Closed = true
	close(n.CBoard)
	close(n.CThread)
	close(n.CPost)
	close(n.CFile)
	n.Storage.Close()
}

func parseFlags() *NodeConfig {
	c := new(NodeConfig)
	hostname, _ := os.Hostname()
	flag.Bool(                         "node",          false,                   "Node : Enable node process.")
	var etcdEndpoints = flag.String(   "etcd",          "http://127.0.0.1:2379", "Node : Etcd addresses (comma-delimited)")
	var cassEndpoints = flag.String(   "cassandra",     "127.0.0.1",             "Node : Cassandra addresses (comma-delimited)")
	var onlyBoards    = flag.String(   "onlyboards",    "",                      "Node : Boards (a,b,sp) to process. Comma seperated.")
	var excludeBoards = flag.String(   "excludeboards", "",                      "Node : Boards (a,b,sp) to exclude. Comma seperated.")
	flag.IntVar(   &(c.ThreadWorkers), "tw",            10,                      "Node : Number of concurrent thread downloaders.")
	flag.StringVar(&(c.Hostname),      "hostname",      hostname,                "Node : Hostname or ip, visible from other machines on the network. ")
	flag.StringVar(&(c.BindIp),        "bindip",        "127.0.0.1",             "Node : Address to bind to.")
	flag.StringVar(&(c.CassKeyspace),  "keyspace",      "chan",                  "Node : Cassandra keyspace")
	flag.StringVar(&(c.ClusterName),   "clustername",   "streamingchan",         "Node : Cluster name")
	flag.IntVar(   &(c.HttpPort),      "httpport",      3333,                    "Node : Host for HTTP Server for serving stats. 0 for disabled.")
	flag.BoolVar(  &(c.NoFiles),       "nofiles",       false,                   "Node : Don't download files")
	flag.BoolVar(  &(c.Verbose),       "verbose",       false,                   "Node : Verbose logging")
	flag.Parse()

	c.EtcdEndpoints = strings.Split(*etcdEndpoints, ",")
	c.CassEndpoints = strings.Split(*cassEndpoints, ",")
	if len(*onlyBoards) > 0 {
		c.OnlyBoards    = strings.Split(*onlyBoards,    ",")
	}
	if len(*excludeBoards) > 0 {
		c.ExcludeBoards = strings.Split(*excludeBoards, ",")
	}
	c.CmdLine = strings.Join(os.Args, " ")
	return c
}

func NewNode(stop chan<- bool) *Node {
	n := new(Node)
	n.stop = stop
	n.Stats = NewNodeStats()
	n.Config = parseFlags()
	n.Storage = fourchan.NewStorage(n.Config.CassKeyspace, n.Config.CassEndpoints...)
	cfg := etcd.Config {
		Endpoints:               n.Config.EtcdEndpoints,
		Transport:               etcd.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := etcd.New(cfg)
	if err != nil {
		log.Fatal("Failed to connected to etcd: ", err)
	}
	n.Keys = etcd.NewKeysAPI(c)
	n.Shutdown = false
	n.Closed = false
	return n
}

func (n *Node) refreshNode() {
	log.Print("Refreshing node ", n.Info.Id)
	_, err := n.Keys.Set(
		context.Background(),
		fmt.Sprintf("/%s/nodes/%s", n.Config.ClusterName, n.Info.Id),
		"",
		&etcd.SetOptions{TTL: nodeLockTTL, Refresh: true})

	if err != nil {
		log.Fatalf("Couldn't refresh node %s", n.Info.Id)
	}
}

func (n *Node) refreshNodeLoop() {
	ticker := time.NewTicker(nodeLockTTL / 2)
	for {
		select {
			case <-ticker.C:
				n.refreshNode()
		}
	}
}

func (n *Node) Bootstrap() error {
	log.Print("Bootstrap routine started")

	n.Id = uuid.NewV4().String()
	n.Info = &NodeInfo{n.Id, n.Config.Hostname}
	nodeInfoJson, _ := json.Marshal(n.Info)
	log.Printf("Creating node %s\n", n.Info.Id)

	_, err := n.Keys.Set(
		context.Background(),
		fmt.Sprintf("/%s/nodes/%s", n.Config.ClusterName, n.Info.Id),
		string(nodeInfoJson),
		&etcd.SetOptions{TTL: nodeLockTTL})

	if err != nil {
		return err
	}

	log.Print("Bootstrap routine completed")

	go n.refreshNodeLoop()
	go n.topologyWatcher()

	log.Print("Downloading boards list...")
	n.Boards, err = fourchan.DownloadBoards(n.Config.OnlyBoards, n.Config.ExcludeBoards)
	log.Printf("%+v", n.Boards)
	if err != nil {
		return err
	}

	go n.statusServer()

	n.LMDelay = 2 * time.Second / time.Duration(len(n.Boards.Boards))
	n.CThread = make(chan *fourchan.ThreadInfo, 4)
	n.CPost = make(chan *fourchan.Post, 8)
	n.CFile = make(chan *fourchan.File, 4)

	n.startBoardRoutines()
	n.startThreadRoutines()
	n.startPostRoutines()
	n.startFileRoutines()

	return nil
}

func (n *Node) topologyWatcher() {
	log.Println("Watching node topology for changes")

	watcher := n.Keys.Watcher(
		fmt.Sprintf("/%s/nodes", n.Config.ClusterName), 
		&etcd.WatcherOptions{Recursive: true})

	for {
		resp, err := watcher.Next(context.Background())
		if err != nil {
			log.Print("watcher failed:", err)
			continue
		}
		log.Printf("node topology updated (%s)\n", resp.Action)
	}
	// coordination stuff
	/*nodesKey := fmt.Sprintf("/%s/nodes", n.Config.ClusterName)
	resp, err := n.Keys.Get(
		context.Background(),
		nodesKey,
		&etcd.GetOptions{Recursive: true, Quorum: true})
	}*/
	// end coordination stuff
}

func (n *Node) CleanShutdown() {
	if n.Shutdown {
		return
	}
	n.Shutdown = true
	log.Print("Removing node from cluster.")
	// TODO remove node from cluster
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(120 * time.Second)
		if false {
			log.Print("Timeout waiting for sockets.")
			stack := make([]byte, 262144)
			runtime.Stack(stack, true)
			log.Print("----------- DUMP STACK CALLED ----------------")
			log.Print("\n", string(stack))
		}
		timeout <- true
	}()
	go func() {
		log.Print("Closing...")
		n.Close()
		log.Print("Wait for routines to finish...")
		// TODO: wait group(s)?
		log.Print("Shut down node.")
		timeout <- true
	}()
	<-timeout
}
