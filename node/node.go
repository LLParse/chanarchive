package node

import (
	"bytes"
	"crypto/md5"
	crand "crypto/rand"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
  "github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
  etcd "github.com/coreos/etcd/client"
	"github.com/pebbe/zmq4"
	"github.com/llparse/streamingchan/fourchan"
	"io"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	THRD_PUB_PORT_LOW  = 40000
	THRD_PUB_PORT_HIGH = 50000
	POST_PUB_PORT_LOW  = 30000
	POST_PUB_PORT_HIGH = 40000
	THREAD_WORKERS     = 10
)

type NodeInfo struct {
	NodeId    string `json:"nodeid"`
	NodeIndex int    `json:"nodeindex"`
	Hostname  string `json:"hostname"`
	TPubPort  int    `json:"thread_pub_port"`
	PPubPort  int    `json:"post_pub_port"`
}

type NodeConfig struct {
	Hostname      string
	BindIp        string
	OnlyBoards    []string
	ExcludeBoards []string
	Cluster       string
	ThreadWorkers int
}

type Node struct {
	NodeId           string
	Stats            *NodeStats
	Boards           *fourchan.Boards
	Storage          *fourchan.Storage
	Config           NodeConfig
	EtcKeys          etcd.KeysAPI
	ThreadPubSocket  *zmq4.Socket
	ThreadPub        chan fourchan.ThreadInfo
	ThreadSubSocket  *zmq4.Socket
	ThreadSub        chan fourchan.ThreadInfo
	PostPubSocket    *zmq4.Socket
	PostPub          chan fourchan.Post
	FilePub          chan *fourchan.File
	BoardStop        []chan bool
	Shutdown         bool
	Closed           bool
	SocketWaitGroup  sync.WaitGroup
	RoutineWaitGroup sync.WaitGroup
	OwnedBoards      []string
	LastNodeIdx      int
	NodeCount        int
	DivideMutex      sync.Mutex
	Info             *NodeInfo
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

func randInt(min, max int) int {
	return int(rand.Float64()*float64(max-min)) + min
}

func (n *Node) Close() {
	n.Closed = true
	if n.ThreadSub != nil {
		go func() { _, _ = <-n.ThreadSub }()
		time.Sleep(5 * time.Millisecond)
		close(n.ThreadSub)
	}
	n.ThreadSub = nil
	if n.ThreadPub != nil {
		go func() { _, _ = <-n.ThreadPub }()
		time.Sleep(5 * time.Millisecond)
		close(n.ThreadPub)
	}
	n.ThreadPub = nil
	if n.PostPub != nil {
		go func() { _, _ = <-n.PostPub }()
		time.Sleep(5 * time.Millisecond)
		close(n.PostPub)
	}
	n.PostPub = nil
	if n.FilePub != nil {
		go func() { _, _ = <-n.FilePub }()
		time.Sleep(5 * time.Millisecond)
		close(n.FilePub)
	}
	n.FilePub = nil
	n.Storage.Close()
	//n.ThreadSubSocket.Close()
	//n.ThreadPubSocket.Close()
	//n.PostPubSocket.Close()
}

func NewNode(flags *FlagConfig) *Node {
	n := new(Node)
	n.Stats = NewNodeStats()
	n.Config.Hostname = flags.Hostname
	n.Config.BindIp = flags.BindIp
	n.Config.Cluster = flags.ClusterName
	if flags.OnlyBoards != "" {
		n.Config.OnlyBoards = strings.Split(flags.OnlyBoards, ",")
	}
	if flags.ExcludeBoards != "" {
		n.Config.ExcludeBoards = strings.Split(flags.ExcludeBoards, ",")
	}
	n.Storage = fourchan.NewStorage(flags.CassKeyspace, strings.Split(flags.CassEndpoints, ",")...)
	cfg := etcd.Config {
		Endpoints:               strings.Split(flags.EtcdEndpoints, ","),
		Transport:               etcd.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := etcd.New(cfg)
	if err != nil {
		log.Fatal("Failed to connected to etcd: ", err)
	}
	n.EtcKeys = etcd.NewKeysAPI(c)

	n.Shutdown = false
	n.Closed = false
	return n
}

func (n *Node) rollbackBootstrap() {
	if n.ThreadSubSocket != nil {
		n.ThreadSubSocket.Close()
		n.ThreadSubSocket = nil
		n.SocketWaitGroup.Done()
	}

	if n.ThreadPubSocket != nil {
		n.ThreadPubSocket.Close()
		n.ThreadPubSocket = nil
		n.SocketWaitGroup.Done()
	}

	if n.PostPubSocket != nil {
		n.PostPubSocket.Close()
		n.PostPubSocket = nil
		n.SocketWaitGroup.Done()
	}
}

func (n *Node) Bootstrap() error {
	log.Print("Bootstrapping node...")
	n.NodeId = randNodeId()
	log.Print("Node Id:", n.NodeId)

	log.Print("Downloading 4Chan boards list...")
	var err error
	n.Boards, err = fourchan.DownloadBoards(n.Config.OnlyBoards, n.Config.ExcludeBoards)
	log.Printf("%+v", n.Boards)
	if err != nil {
		log.Print("Failed to download boards list: ", err)
		return err
	}
	for _, board := range n.Boards.Boards {
		n.Storage.PersistBoard(&board)
	}

	if n.Config.Hostname == "" {
		n.Config.Hostname, _ = os.Hostname()
	}

	log.Print("Initializing publisher sockets...")
	tpubport := randInt(THRD_PUB_PORT_LOW, THRD_PUB_PORT_HIGH)
	ppubport := randInt(POST_PUB_PORT_LOW, POST_PUB_PORT_HIGH)
	if n.ThreadPubSocket, err = zmq4.NewSocket(zmq4.PUB); err != nil {
		log.Print("Failed to create Thread Pub Socket: ", err)
		return err
	}
	if n.PostPubSocket, err = zmq4.NewSocket(zmq4.PUB); err != nil {
		log.Print("Failed to create Post Pub Socket: ", err)
		return err
	}

	if err := n.ThreadPubSocket.Bind(fmt.Sprintf("tcp://%s:%d", n.Config.BindIp, tpubport)); err != nil {
		log.Print((fmt.Sprintf("tcp://%s:%d", n.Config.BindIp, tpubport)))
		log.Print("Failed to bind Thread Pub Socket: ", err)
		return err
	}
	if err := n.PostPubSocket.Bind(fmt.Sprintf("tcp://%s:%d", n.Config.BindIp, ppubport)); err != nil {
		log.Print("Failed to bind Post Sub Socket: ", err)
		return err
	}
	n.SocketWaitGroup.Add(2)

	nodepath := "/" + n.Config.Cluster + "/nodes"
	for tries := 0; tries < 10; tries++ {
		log.Print("Registering to etcd...")
		resp, err := n.EtcKeys.Get(context.Background(), nodepath, nil)
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
		n.Info = &NodeInfo{n.NodeId, nodeIndex, n.Config.Hostname, tpubport, ppubport}
		nodes = append(nodes, *n.Info)
		if n.ThreadSubSocket, err = zmq4.NewSocket(zmq4.SUB); err != nil {
			log.Print("Failed to create Thread Sub Socket: ", err)
			return err
		}
		n.ThreadSubSocket.SetSubscribe("")
		n.SocketWaitGroup.Add(1)
		var failureErr error
		failureErr = nil
		for _, node := range nodes {
			log.Print("Connecting to ", fmt.Sprintf("tcp://%s:%d", node.Hostname, node.TPubPort))
			if err := n.ThreadSubSocket.Connect(fmt.Sprintf("tcp://%s:%d", node.Hostname, node.TPubPort)); err != nil {
				log.Print(err)
				failureErr = err
				break
			}
		}
		if failureErr != nil {
			log.Print("Failed to create Thread Sub Socket: ", err)
			time.Sleep(2 * time.Second)
			n.rollbackBootstrap()
			continue
		}
		log.Print("Finished connecting, watching nodes...")

		n.ThreadPub = make(chan fourchan.ThreadInfo)
		n.PostPub = make(chan fourchan.Post)
		n.ThreadSub = make(chan fourchan.ThreadInfo)
		n.FilePub = make(chan *fourchan.File)

		go n.addNodeWatcher()
		go n.topologyWatcher()

		sort.Sort(NodeInfoList(nodes))
		for idx, _ := range nodes {
			nodes[idx].NodeIndex = idx + 1
		}

		newNodeData, _ := json.Marshal(nodes)
		time.Sleep(1 * time.Second)
		log.Print("Updating node list.")

		_, err = n.EtcKeys.Set(context.Background(), nodepath, string(newNodeData), nil)
		if err != nil {
			log.Print("Failed to update node list. Possibly another node bootstrapped before finish.")
			//n.Close()
			n.rollbackBootstrap()
			continue
		}

		go n.bootstrapThreadWorkers()

		go func() {
			for !n.Closed {
				time.Sleep(1 * time.Millisecond)
				data, err := n.ThreadSubSocket.RecvBytes(zmq4.DONTWAIT)
				if err == syscall.EAGAIN {
					time.Sleep(1 * time.Millisecond)
					continue
				}
				var threadInfo fourchan.ThreadInfo
				dec := gob.NewDecoder(bytes.NewBuffer(data))
				err = dec.Decode(&threadInfo)
				if err != nil {
					//log.Print(data)
					//log.Print("Failed to decode thread gob ", err)
					continue
				}
				if n.ThreadSub != nil {
					n.ThreadSub <- threadInfo
				}
			}
			n.ThreadSubSocket.Close()
			n.SocketWaitGroup.Done()
		}()

		go n.threadPublisher()
		go n.postPublisher()
		go n.filePublisher()

		opts := &etcd.SetOptions{TTL: 600}
		n.EtcKeys.Set(context.Background(), n.Config.Cluster+"/subscribe-thread/"+n.NodeId,
			fmt.Sprintf("%s:%d", n.Config.Hostname, n.Info.TPubPort), opts)
		n.EtcKeys.Set(context.Background(), n.Config.Cluster+"/subscribe-post/"+n.NodeId,
			fmt.Sprintf("%s:%d", n.Config.Hostname, n.Info.PPubPort), opts)
		log.Print("Complete bootstrapping node.")
		return nil
	}

	log.Print("Failed to bootstrap node.")
	return errors.New("Failed to bootstrap node.")
}

func (n *Node) addNodeWatcher() {
	path := "/" + n.Config.Cluster + "/subscribe-thread"
	watcher := n.EtcKeys.Watcher(path, nil)
	log.Print(path, " watcher registered")
	for {
		resp, err := watcher.Next(context.Background())
		if err != nil {
			log.Print(path, " watcher failure:", err)
			continue
		}
		if resp.Action == "set" {
			if fmt.Sprintf("%s:%d", n.Config.Hostname, n.Info.TPubPort) != resp.Node.Value {
				address := fmt.Sprintf("tcp://%s", resp.Node.Value)
				log.Print("Connecting to ", address)
				e := n.ThreadSubSocket.Connect(address)
				if e != nil {
					log.Print("Failed to connect to ", address, ":", e)
				}
			}
		}
	}
}

func (n *Node) topologyWatcher() {
	path := "/" + n.Config.Cluster + "/nodes"
	watcher := n.EtcKeys.Watcher(path, nil)
	for {
		resp, err := watcher.Next(context.Background())
		if err != nil {
			log.Print(path, " watcher failure:", err)
			continue
		}
		if resp.Action == "set" {
			log.Print(path, " updated, dividing boards...")
			n.divideBoards()
		}		
	}
}

func (n *Node) threadPublisher() {
	for threadInfo := range n.ThreadPub {
		//log.Printf("publisher received thread: %+v", threadInfo)
		n.Storage.PersistThread(&threadInfo)
		var buff bytes.Buffer
		enc := gob.NewEncoder(&buff)
		err := enc.Encode(threadInfo)
		if err != nil {
			log.Print("Failed to encode threadInfo ", err)
			continue
		}
		for tries := 0; tries < 16; tries++ {
			_, e := n.ThreadPubSocket.SendBytes(buff.Bytes(), zmq4.DONTWAIT)
			if e == syscall.EAGAIN {
				time.Sleep(1 * time.Millisecond)
				continue
			} else if e == nil {
				break
			}
		}
	}
	n.ThreadPubSocket.Close()
	n.SocketWaitGroup.Done()
}

func (n *Node) postPublisher() {
	for post := range n.PostPub {
		n.Storage.PersistPost(&post)
		if post.Md5 != "" && post.Ext != "" && !n.Closed {
			file := &fourchan.File{
				Board: post.Board,
				Tim: post.Tim,
				Md5: post.Md5,
				Ext: post.Ext,
				FSize: post.FSize,
			}
			n.FilePub <- file
		}

		var buff bytes.Buffer
		enc := gob.NewEncoder(&buff)
		err := enc.Encode(post)
		if err != nil {
			log.Print("Failed to encode post ", err)
			continue
		}
		for tries := 0; tries < 16; tries++ {
			_, e := n.PostPubSocket.SendBytes(buff.Bytes(), zmq4.DONTWAIT)
			if e == syscall.EAGAIN {
				time.Sleep(1 * time.Millisecond)
				continue
			} else if e == nil {
				break
			}
		}
	}
	n.PostPubSocket.Close()
	n.SocketWaitGroup.Done()
}

func (n *Node) filePublisher() {
	for file := range n.FilePub {
		if !n.Storage.FileExists(file) {
			data, err := fourchan.DownloadFile(file.Board, file.Tim, file.Ext)
			if err == nil {
				file.Data = data
				n.Storage.PersistFile(file)
			} else {
				log.Printf("Error downloading file %+v: %+v", file, err)
			}
		} else {
			log.Printf("File exists: %+v", file)
		}
	}
}

func (n *Node) divideBoards() {
	n.DivideMutex.Lock()
	defer n.DivideMutex.Unlock()
	if n.BoardStop != nil {
		for _, c := range n.BoardStop {
			c <- true
		}
	}
	resp, err := n.EtcKeys.Get(context.Background(), n.Config.Cluster + "/nodes", nil)
	if err == nil {
		var nodes []NodeInfo
		result := resp.Node
		if e := json.Unmarshal([]byte(result.Value), &nodes); e != nil {
			log.Print("Failed to unmarshhal " + n.Config.Cluster + "/nodes")
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
			_, err := n.EtcKeys.Set(context.Background(), n.Config.Cluster+"/nodes", string(newNodeData), 
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
}

func (n *Node) bootstrapThreadWorkers() {
	var wg sync.WaitGroup
	for i := 0; i < THREAD_WORKERS; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ti := range n.ThreadSub {
				if ti.OwnerId == n.NodeId {
					//log.Printf("Process thread %+v", ti)
					n.ProcessThread(ti)
				}
			}
		}()
	}

	wg.Wait()
}

func (n *Node) CleanShutdown() {
	if n.Shutdown {
		return
	}
	n.Shutdown = true
	log.Print("Removing node from cluster.")
	for tries := 0; tries < 32; tries++ {
		resp, err := n.EtcKeys.Get(context.Background(), n.Config.Cluster+"/nodes", nil)
		if err == nil {
			var nodes []NodeInfo
			result := resp.Node
			if e := json.Unmarshal([]byte(result.Value), &nodes); e != nil {
				log.Print("Failed to unmarshal " + n.Config.Cluster + "/nodes")
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
			_, err = n.EtcKeys.Set(context.Background(), n.Config.Cluster+"/nodes", string(newNodeData), &etcd.SetOptions{PrevValue: result.Value})
			if err != nil {
				log.Print("Failed to update node list. Possibly another node bootstrapped before finish.")
				continue
			}
			_, err = n.EtcKeys.Set(context.Background(), n.Config.Cluster+"/nodes/remove/"+n.NodeId, fmt.Sprintf("%s", n.NodeId), &etcd.SetOptions{TTL: time.Second * 60})
			if err != nil {
				log.Printf("Failed to set /nodes/remove/%s: %+v", n.NodeId, err)
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
		log.Print("Closing sockets...")
		n.Close()
		n.SocketWaitGroup.Wait()
		log.Print("Cleaning up...")
		n.RoutineWaitGroup.Wait()
		log.Print("Shut down node.")
		timeout <- true
	}()
	<-timeout
}
