package api

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
  "github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
  etcd "github.com/coreos/etcd/client"
	"github.com/pebbe/zmq4"
	"github.com/llparse/streamingchan/fourchan"
	"github.com/llparse/streamingchan/node"
	"github.com/llparse/streamingchan/version"
	"log"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	PORT_NUMBER     = 4000
	MAX_CONNECTIONS = 64
)

type ApiConfig struct {
	CmdLine        string
	PortNumber     int
	EtcdEndpoints  []string
	ClusterName    string
	MaxConnections int
}

type ApiServer struct {
	Stats         *node.NodeStats
	Config        ApiConfig
	stop          chan<- bool
	EtcKeys       etcd.KeysAPI
	PostPubSocket *zmq4.Socket
	Listeners     []chan fourchan.Post
	Connections   int64
	Storage       *fourchan.Storage
}

func (as *ApiServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	stats := map[string]interface{}{
		"posts_processed": map[string]interface{}{
			"5min": as.Stats.Aggregate(node.METRIC_POSTS, node.TIME_5SEC, 60),
			"1hr":  as.Stats.Aggregate(node.METRIC_POSTS, node.TIME_1MIN, 60),
			"1d":   as.Stats.Aggregate(node.METRIC_POSTS, node.TIME_1HOUR, 24),
		},
		"new_connections": map[string]interface{}{
			"1hr": as.Stats.Aggregate(node.METRIC_CONNECTIONS, node.TIME_1MIN, 60),
			"6hr": as.Stats.Aggregate(node.METRIC_CONNECTIONS, node.TIME_1HOUR, 6),
			"1d":  as.Stats.Aggregate(node.METRIC_CONNECTIONS, node.TIME_1HOUR, 24),
			"3d":  as.Stats.Aggregate(node.METRIC_CONNECTIONS, node.TIME_1HOUR, 73),
		},
	}
	data := map[string]interface{}{
		"ok":                   1,
		"revision":             version.GitHash,
		"build_date":           version.BuildDate,
		"cmd_line":             as.Config.CmdLine,
		"runtime":              time.Since(as.Stats.StartTime),
		"memory":               memStats.Alloc,
		"processId":            os.Getpid(),
		"posts_processed":      as.Stats.Lifetime.Posts,
		"lifetime_connections": as.Stats.Lifetime.Connections,
		"stats":                stats,
		"connections":          as.Connections,
	}
	//log.Print("Serving ", r.URL.Path)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data["runtime"] = (data["runtime"].(time.Duration)).Seconds()
	out, err := json.Marshal(data)
	if err == nil {
		fmt.Fprint(w, string(out))
	}
}

func (as *ApiServer) commandHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if strings.Contains(r.URL.Path, "/commands/stop") {
		p := map[string]interface{}{
			"ok":      1,
			"message": "Stop command sent, check logs",
		}
		data, _ := json.Marshal(p)
		fmt.Fprint(w, string(data))
		go as.shutdown()
	} else {
		p := map[string]interface{}{
			"ok":      0,
			"message": "Invalid command",
		}
		data, _ := json.Marshal(p)
		fmt.Fprint(w, string(data))
		return
	}
}

func (as *ApiServer) shutdown() {
	log.Print("Recieved command to shutdown. Shutting down in 2 seconds.")
	time.Sleep(2 * time.Second)
	as.stop <- true
}

func (as *ApiServer) streamHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	values := r.URL.Query()
	filters := make([]Filter, 0, 16)
	for k, vs := range values {
		for _, v := range vs {
			switch k {
			case "board":
				filters = append(filters, Filter{Board: v})
			case "thread":
				spl := strings.Split(v, "/")
				if len(spl) != 2 {
					continue
				}
				b := spl[0]
				if t, e := strconv.Atoi(spl[1]); e == nil {
					filters = append(filters, Filter{Board: b, Thread: t})
				}
			case "comment":
				filters = append(filters, Filter{Comment: v})
			case "name":
				filters = append(filters, Filter{Name: v})
			case "trip":
				filters = append(filters, Filter{Trip: v})
			}
		}
	}
	if as.Connections >= int64(as.Config.MaxConnections) && as.Config.MaxConnections != -1 {
		w.WriteHeader(400)
		p := map[string]interface{}{
			"ok":      0,
			"message": "Maximum connections reached.",
		}
		data, _ := json.Marshal(p)
		fmt.Fprint(w, string(data))
		return
	}

	as.Stats.Incr(node.METRIC_CONNECTIONS, 1)
	atomic.AddInt64(&as.Connections, 1)
	defer atomic.AddInt64(&as.Connections, -1)

	subSocket, err := zmq4.NewSocket(zmq4.SUB)
	if err != nil {
		w.WriteHeader(500)
		p := map[string]interface{}{
			"ok":      0,
			"message": "Server Error",
		}
		data, _ := json.Marshal(p)
		fmt.Fprint(w, string(data))
		log.Print("Failed to create Sub Socket: ", err)
		return
	}
	resp, err := as.EtcKeys.Get(context.Background(), as.Config.ClusterName + "/nodes", nil)
	if err != nil {
		w.WriteHeader(500)
		p := map[string]interface{}{
			"ok":      0,
			"message": "Server Error",
		}
		data, _ := json.Marshal(p)
		fmt.Fprint(w, string(data))
		log.Print("Failed to contact etcd: ", err)
		return
	}

	var nodes []node.NodeInfo
	result := resp.Node
	if e := json.Unmarshal([]byte(result.Value), &nodes); e != nil {
		w.WriteHeader(500)
		p := map[string]interface{}{
			"ok":      0,
			"message": "Server Error",
		}
		data, _ := json.Marshal(p)
		fmt.Fprint(w, string(data))
		log.Print("Failed to unmarshal nodes: ", err)
		return
	}

	for _, nodeInfo := range nodes {
		if err := subSocket.Connect(fmt.Sprintf("tcp://%s:%d", nodeInfo.Hostname, nodeInfo.PPubPort)); err != nil {
			log.Print(err)
		}
	}

	go func() {
		path := "/" + as.Config.ClusterName + "/subscribe-post"
		watcher := as.EtcKeys.Watcher(path, nil)
		for {
			resp, err := watcher.Next(context.Background())
			if err != nil {
				log.Print("Add PostPub watch failure", err)
				continue
			}
			if err = subSocket.Connect(fmt.Sprintf("tcp://%s", resp.Node.Value)); err != nil {
				log.Print(err)
			}
		}
	}()

/*func (as *ApiServer) subscribePostWatcher() {
	path := "/" + as.Config.ClusterName + "/subscribe-post"
	watcher := as.EtcKeys.Watcher(path, nil)
	log.Print(path, " watcher registered")
	for {
		resp, err := watcher.Next(context.Background())
		if err != nil {
			log.Print(path, " watcher failure:", err)
			continue
		}
		if err = subSocket.Connect(fmt.Sprintf("tcp://%s", resp.Node.Value)); err != nil {
			log.Print(err)
		}
	}
}*/


	subSocket.SetSubscribe("")
	w.WriteHeader(200)
	lastMessage := time.Now()
	messageSent := false
	for {
		time.Sleep(1 * time.Millisecond)
		if time.Now().Sub(lastMessage) > (30 * time.Second) {
			m := "{}\r\n"
			if messageSent {
				m = "\r\n"
			}
			if _, err := fmt.Fprint(w, m); err != nil {
				break
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			messageSent = true
			lastMessage = time.Now()
		}
		data, err := subSocket.RecvBytes(zmq4.DONTWAIT)
		if err == syscall.EAGAIN {
			time.Sleep(1 * time.Millisecond)
			continue
		}
		var post fourchan.Post
		dec := gob.NewDecoder(bytes.NewBuffer(data))
		err = dec.Decode(&post)
		if err != nil {
			//log.Print("Failed to decode thread post ", err)
			continue
		}
		passed := true
		for _, filter := range filters {
			if !filter.Passes(post) {
				passed = false
				break
			}
		}
		if !passed {
			continue
		}
		jdata, _ := json.Marshal(post)
		d := strings.Replace(string(jdata), "\r\n", "\n", -1) + "\n"
		if _, err := fmt.Fprint(w, d); err != nil {
			break
		}
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		as.Stats.Incr(node.METRIC_POSTS, 1)
		lastMessage = time.Now()
		messageSent = true
	}
	subSocket.Close()
	//stop <- true
}

func (as *ApiServer) boardHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
}

func NewApiServer(flags *FlagConfig, stop chan<- bool) *ApiServer {
	as := new(ApiServer)
	as.Config.CmdLine = strings.Join(os.Args, " ")
	as.Config.PortNumber = flags.HttpPort
	as.Config.MaxConnections = flags.MaxConnections
	as.Config.ClusterName = flags.ClusterName
	as.Config.EtcdEndpoints = strings.Split(flags.EtcdEndpoints, ",")
	as.stop = stop
	as.Stats = node.NewNodeStats()
	as.Storage = fourchan.NewStorage(flags.CassKeyspace, strings.Split(flags.CassEndpoints, ",")...)
	cfg := etcd.Config {
		Endpoints:               as.Config.EtcdEndpoints,
		Transport:               etcd.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}
	c, err := etcd.New(cfg)
	if err != nil {
		log.Fatal("Failed to connected to etcd: ", err)
		return nil
	}
	as.EtcKeys = etcd.NewKeysAPI(c)
	return as
}

func (as *ApiServer) Serve() error {
	log.Println("Starting HTTP Server on port", as.Config.PortNumber)
	http.HandleFunc("/status/", as.statusHandler)
	http.HandleFunc("/commands/", as.commandHandler)
	http.HandleFunc("/stream.json", as.streamHandler)
	http.HandleFunc("/boards/", as.boardHandler)
	if e := http.ListenAndServe(fmt.Sprintf(":%d", as.Config.PortNumber), nil); e != nil {
		log.Print("Error starting server", e)
		return e
	}
	return nil
}
