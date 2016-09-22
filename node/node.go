package node

import (
	"encoding/json"
  "golang.org/x/net/context"
  etcd "github.com/coreos/etcd/client"
	"github.com/llparse/streamingchan/fourchan"
	"github.com/satori/go.uuid"
	"log"
	"os"
	"strings"
	"sync"
	"time"
	"flag"
	"fmt"
)

type Node struct {
	Id               string
	Lock             *EtcdLock
	Stats            *NodeStats
	Boards           *fourchan.Boards
	Storage          *fourchan.Storage
	Config           *NodeConfig
	Keys             etcd.KeysAPI
	LMDelay          time.Duration
	CBoard           chan *fourchan.Board
	CThread          chan *fourchan.Thread
	CPost            chan *fourchan.Post
	CFile            chan *fourchan.File
	BoardStop        []chan bool
	Closed           bool
	OwnedBoards      []string
	LastNodeIdx      int
	NodeCount        int
	DivideMutex      sync.Mutex
	Info             *NodeInfo
	InfoJson         string
	stop             chan bool
	stopBoard        chan bool
	stopThread       chan bool
	stopPost         chan bool
	stopFile         chan bool
	Files            map[int]string
	FileMutex        sync.Mutex
}

type NodeInfo struct {
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
	nodeLockTTL = 60 * time.Second
  boardLockTTL = 20 * time.Second
  numBoardRoutines = 1
  numThreadRoutines = 4
  numPostRoutines = 32
  numFileRoutines = 8
)

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

func NewNode(stop chan bool) *Node {
	n := new(Node)
	n.stop = stop
	n.stopBoard = make(chan bool, 2)
	n.stopThread = make(chan bool, 2)
	n.stopPost = make(chan bool, 2)
	n.stopFile = make(chan bool, 2)
	n.Stats = NewNodeStats()
	n.Config = parseFlags()
	n.Storage = fourchan.NewStorage(n.Config.CassKeyspace, n.Config.CassEndpoints...)
	cfg := etcd.Config {
		Endpoints:               n.Config.EtcdEndpoints,
		Transport:               etcd.DefaultTransport,
		HeaderTimeoutPerRequest: 3 * time.Second,
	}
	c, err := etcd.New(cfg)
	if err != nil {
		log.Fatal("Failed to connected to etcd: ", err)
	}
	n.Keys = etcd.NewKeysAPI(c)
	n.Closed = false
	// TODO these chan sizes are rather arbitrary...
	n.CBoard = make(chan *fourchan.Board, numBoardRoutines)
	n.CThread = make(chan *fourchan.Thread, numThreadRoutines)
	n.CPost = make(chan *fourchan.Post, numPostRoutines)
	n.CFile = make(chan *fourchan.File, numFileRoutines)
  n.Files = make(map[int]string)

	return n
}

func (n *Node) Bootstrap() {
	log.Print("Bootstrapping started")

	n.Id = uuid.NewV4().String()
	n.Info = &NodeInfo{n.Config.Hostname}
	
	nodeInfoJson, _ := json.Marshal(n.Info)
	n.InfoJson = string(nodeInfoJson)

  n.Lock = n.NewEtcdLock(
    fmt.Sprintf("/%s/nodes/%s", n.Config.ClusterName, n.Id),
    n.InfoJson,
    nodeLockTTL)

  if err := n.Lock.Acquire(); err != nil {
  	log.Fatal(err)
  }

	n.watchNodes()
	n.downloadBoards()
	n.statusServer()
	log.Print("Bootstrapping completed")
}

func (n *Node) Run() {
	var wg sync.WaitGroup
	wg.Add(4)
	go n.startBoardRoutines(&wg)
	go n.startThreadRoutines(&wg)
	go n.startPostRoutines(&wg)
	go n.startFileRoutines(&wg)
	wg.Wait()
	
	log.Print("All routines finished, shutting down.")
	n.Shutdown()
}

func (n *Node) watchNodes() {
	log.Println("Watching node topology for changes")

	watcher := n.Keys.Watcher(
		fmt.Sprintf("/%s/nodes", n.Config.ClusterName), 
		&etcd.WatcherOptions{Recursive: true})

	go func() {
		for {
			resp, err := watcher.Next(context.Background())
			if err != nil {
				log.Print("watcher failed:", err)
				continue
			}
			log.Printf("node topology updated (%s)\n", resp.Action)
		}
	}()
}

func (n *Node) Shutdown() {
	n.Storage.Close()
	log.Print("Removing node from cluster.")
	n.Lock.Release()
	log.Print("Shut down complete.")
}
