package node

import (
	"crypto/md5"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
  "github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
  etcd "github.com/coreos/etcd/client"
	"github.com/llparse/streamingchan/fourchan"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
	"flag"
	"net/http"
)

type Node struct {
	NodeId           string
	Stats            *NodeStats
	Boards           *fourchan.Boards
	Storage          *fourchan.Storage
	Config           *NodeConfig
	Keys             etcd.KeysAPI
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
	NodeId    string `json:"nodeid"`
	NodeIndex int    `json:"nodeindex"`
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
}


func randNodeId() string {
	h := md5.New()

	b := make([]byte, 1024)
	n, err := io.ReadFull(crand.Reader, b)
	if n != len(b) || err != nil {
		panic(err)
		return ""
	}
	if sz, err := h.Write(b); err != nil {
		panic(err)
		return ""
	} else {
		if sz != n {
			panic("Failed to write random bytes to hash?")
		}
		return hex.EncodeToString(h.Sum(nil))
	}
	panic("")
}

func (n *Node) Close() {
	n.Closed = true
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
	flag.Parse()

	c.EtcdEndpoints = strings.Split(*etcdEndpoints, ",")
	c.CassEndpoints = strings.Split(*cassEndpoints, ",")
	c.OnlyBoards    = strings.Split(*onlyBoards,    ",")
	c.ExcludeBoards = strings.Split(*excludeBoards, ",")
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

func (n *Node) Bootstrap() error {
	log.Print("Bootstrapping node...")
	n.NodeId = randNodeId()
	log.Print("Node Id:", n.NodeId)

	nodepath := "/" + n.Config.ClusterName + "/nodes"
	log.Print("Registering to etcd...")
	resp, err := n.Keys.Get(context.Background(), nodepath, nil)
	if err != nil && (err.(etcd.Error)).Code != 100 {
		log.Print("Failed to get ", nodepath, ": ", err)
		return err
	}

	var nodes []NodeInfo
	nodeIndex := 1
	if err == nil {
		if e := json.Unmarshal([]byte(resp.Node.Value), &nodes); e != nil {
			log.Print("Failed to unmarshhal ", nodepath)
			log.Print(resp.Node.Value)
			return e
		}
		for ; nodeIndex <= len(nodes); nodeIndex++ {
			found := false
			for _, node := range nodes {
				if nodeIndex == node.NodeIndex {
					found = true
					break
				}
			}
			if !found {
				break
			}
		}
	} else {
		nodes = make([]NodeInfo, 0, 1)
	}
	n.Info = &NodeInfo{n.NodeId, nodeIndex, n.Config.Hostname}
	nodes = append(nodes, *n.Info)

	log.Print("Finished connecting, watching nodes...")
	go n.topologyWatcher()

	sort.Sort(NodeInfoList(nodes))
	for idx, _ := range nodes {
		nodes[idx].NodeIndex = idx + 1
	}
	newNodeData, _ := json.Marshal(nodes)
	time.Sleep(1 * time.Second)
	log.Print("Updating node list.")
	_, err = n.Keys.Set(context.Background(), nodepath, string(newNodeData), nil)
	if err != nil {
		log.Print("Failed to update node list: ", err)
		return err
	}

	log.Print("Downloading boards list...")
	n.Boards, err = fourchan.DownloadBoards(n.Config.OnlyBoards, n.Config.ExcludeBoards)
	log.Printf("%+v", n.Boards)
	if err != nil {
		log.Print("Failed to download boards list: ", err)
		return err
	}

	go n.statusServer()

	log.Print("Starting processor routines...")
	n.CBoard = make(chan *fourchan.Board, len(n.Boards.Boards) + 1)
	n.CThread = make(chan *fourchan.ThreadInfo, 64)
	n.CPost = make(chan *fourchan.Post, 128)
	n.CFile = make(chan *fourchan.File, 64)

	for _, board := range n.Boards.Boards {
		n.Storage.PersistBoard(board)
		n.CBoard <- board
	}
	
	log.Print("Finished bootstrapping node.")

	go n.boardProcessor(n.CBoard, n.CThread)
	for i := 0; i < 32; i++ {
		go n.threadProcessor(n.CThread, n.CPost)
	}
	for i := 0; i < 32; i++ {
		go n.postProcessor(n.CPost, n.CFile)
	}
	for i := 0; i < 8; i++ {
		go n.fileProcessor(n.CFile)
	}

	return nil

	log.Print("Failed to bootstrap node.")
	return errors.New("Failed to bootstrap node.")
}

func (n *Node) topologyWatcher() {
	path := "/" + n.Config.ClusterName + "/nodes"
	watcher := n.Keys.Watcher(path, nil)
	for {
		resp, err := watcher.Next(context.Background())
		if err != nil {
			log.Print(path, " watcher failure:", err)
			continue
		}
		if resp.Action == "set" {
			log.Print(path, " updated, not dividing boards...")
			//n.divideBoards()
		}		
	}
}

func (n *Node) boardProcessor(boards chan *fourchan.Board, threads chan<- *fourchan.ThreadInfo) {
	for board := range boards {
		// eliminate drift by publishing to channel immediately
		boards <- board
		log.Printf("processing /%s/", board.Board)
		var lastModifiedHeader time.Time
		if t, statusCode, lastModifiedStr, e := fourchan.DownloadBoard(board.Board, lastModifiedHeader); e == nil {
			n.Stats.Incr(METRIC_BOARD_REQUESTS, 1)
			lastModifiedHeader, _ = time.Parse(http.TimeFormat, lastModifiedStr)
			//log.Printf("lm %d: %s", board.LM, board.Board)
			newLastModified := board.LM
			for _, page := range t {
				for _, thread := range page.Threads {
					//if thread.LastModified > board.LM {
						thread.Board = page.Board
						thread.MinPost = board.LM
						thread.OwnerId = n.NodeId
						threads <- thread
						//log.Printf("wrote thread %v", thread)
					//}
					if thread.LastModified > newLastModified {
						newLastModified = thread.LastModified
					}
				}
			}
			if board.LM == newLastModified {
				//log.Printf("board %s didn't change", board.Board)
			} else {
				board.LM = newLastModified
			}
		} else if statusCode != 304 {
			log.Print("Error downloading board ", board.Board, " ", e)
		}
	}
}

func (n *Node) threadProcessor(threads <-chan *fourchan.ThreadInfo, posts chan<- *fourchan.Post) {
	for threadInfo := range threads {
		//log.Printf("processing /%s/thread/%d", threadInfo.Board, threadInfo.No)
		n.Storage.PersistThread(threadInfo)
		if thread, err := fourchan.DownloadThread(threadInfo.Board, threadInfo.No); err == nil {
			n.Stats.Incr(METRIC_THREADS, 1)
			var postNos []int
			for _, post := range thread.Posts {
				if post.Time >= threadInfo.MinPost {
					postNos = append(postNos, post.No)
					posts <- post
					n.Stats.Incr(METRIC_POSTS, 1)
				}
			}
			n.Storage.PersistThreadPosts(threadInfo, postNos)
		} else {
			//log.Print("Error downloading thread: ", err)
		}
	}
}

func (n *Node) postProcessor(posts <-chan *fourchan.Post, files chan<- *fourchan.File) {
	for post := range posts {
		//log.Printf("processing /%s/post/%d", post.Board, post.No)
		n.Storage.PersistPost(post)
		if post.Md5 != "" && post.Ext != "" && !n.Closed {
			file := &fourchan.File{
				Board: post.Board,
				Tim: post.Tim,
				Md5: post.Md5,
				Ext: post.Ext,
				FSize: post.FSize,
			}
			files <- file
		}
	}
}

func (n *Node) fileProcessor(files <-chan *fourchan.File) {
	for file := range files {
		//log.Printf("processing /%s/file/%d", file.Board, file.Tim)
		if !n.Storage.FileExists(file) {
			data, err := fourchan.DownloadFile(file.Board, file.Tim, file.Ext)
			if err == nil {
				file.Data = data
				n.Storage.WriteFile(file)
			} else {
				log.Printf("Error downloading file %+v: %+v", file, err)
			}
		} else {
			//log.Printf("File exists: %+v", file)
		}
	}
}

/*func (n *Node) divideBoards() {
	n.DivideMutex.Lock()
	defer n.DivideMutex.Unlock()
	if n.BoardStop != nil {
		for _, c := range n.BoardStop {
			c <- true
		}
	}
	resp, err := n.Keys.Get(context.Background(), n.Config.ClusterName + "/nodes", nil)
	if err == nil {
		var nodes []NodeInfo
		result := resp.Node
		if e := json.Unmarshal([]byte(result.Value), &nodes); e != nil {
			log.Print("Failed to unmarshhal " + n.Config.ClusterName + "/nodes")
			return
		}
		sort.Sort(NodeInfoList(nodes))
		nodeIds := make([]string, 0, 16)
		for idx, node := range nodes {
			nodes[idx].NodeIndex = idx + 1
			nodeIds = append(nodeIds, node.NodeId)
		}
		newNodeData, _ := json.Marshal(nodes)
		if string(newNodeData) != result.Value {
			_, err := n.Keys.Set(context.Background(), n.Config.ClusterName+"/nodes", string(newNodeData), 
				&etcd.SetOptions{PrevValue: result.Value})
			if err != nil {
				log.Print("Failed to update /nodes data")
			}
			return
		}
		var myNode *NodeInfo
		for idx, node := range nodes {
			if node.NodeId == n.NodeId {
				myNode = &nodes[idx]
			}
		}
		if myNode == nil {
			if n.Shutdown == false {
				log.Print("Oh dear, I can't find myself in the config, I may be dead.")
				go n.CleanShutdown()
			}
			return
		}
		n.BoardStop = make([]chan bool, 0, 64)
		n.OwnedBoards = make([]string, 0, 64)
		n.LastNodeIdx = myNode.NodeIndex
		n.NodeCount = len(nodes)
		for idx, board := range n.Boards.Boards {
			if len(nodes) != 0 && (idx%len(nodes))+1 == myNode.NodeIndex {
				bc := make(chan bool)
				n.BoardStop = append(n.BoardStop, bc)
				n.OwnedBoards = append(n.OwnedBoards, board.Board)
				go n.ProcessBoard(nodeIds, board.Board, bc)
			}
		}
	} else {
		log.Print("Failed to get node configuration.")
		return
	}
}*/

func (n *Node) CleanShutdown() {
	if n.Shutdown {
		return
	}
	n.Shutdown = true
	log.Print("Removing node from cluster.")
	for tries := 0; tries < 32; tries++ {
		resp, err := n.Keys.Get(context.Background(), n.Config.ClusterName+"/nodes", nil)
		if err == nil {
			var nodes []NodeInfo
			result := resp.Node
			if e := json.Unmarshal([]byte(result.Value), &nodes); e != nil {
				log.Print("Failed to unmarshal " + n.Config.ClusterName + "/nodes")
				return
			}
			sort.Sort(NodeInfoList(nodes))
			for idx, _ := range nodes {
				nodes[idx].NodeIndex = idx + 1
			}

			newNodes := make([]NodeInfo, 0, 16)
			for _, node := range nodes {
				if node.NodeId != n.NodeId {
					newNodes = append(newNodes, node)
				}
			}
			newNodeData, _ := json.Marshal(newNodes)
			_, err = n.Keys.Set(context.Background(), n.Config.ClusterName+"/nodes", string(newNodeData), &etcd.SetOptions{PrevValue: result.Value})
			if err != nil {
				log.Print("Failed to update node list. Possibly another node bootstrapped before finish.")
				continue
			}
			break
		} else {
			log.Print("Error getting nodes during clean shutdown")
		}
	}
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
