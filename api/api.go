package api

import (
	"bytes"
	"fmt"
  etcd "github.com/coreos/etcd/client"
	"github.com/llparse/streamingchan/asset"
	"github.com/llparse/streamingchan/fourchan"
	"github.com/llparse/streamingchan/node"
	"github.com/llparse/streamingchan/version"
	"log"
	"net/http"
	"encoding/json"
	"text/template"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
	"flag"
	"io"
)

var t *template.Template

func init() {
	data, err := asset.Asset("asset/templates/thread.html")
	if err != nil {
		log.Fatal("couldn't access asset: ", err)
	}
	t = template.Must(template.New("thread").Parse(string(data)))
}

type ApiConfig struct {
	BindIp         string
	EtcdEndpoints  []string
	CassEndpoints  []string
	CassKeyspace   string
	ClusterName    string
	CmdLine        string
	HttpPort       int
}

type ApiServer struct {
	Stats         *node.NodeStats
	Config        *ApiConfig
	Keys          etcd.KeysAPI
	Listeners     []chan fourchan.Post
	Storage       *fourchan.Storage
	stop          chan<- bool
}

func parseFlags() *ApiConfig {
	c := new(ApiConfig)
	flag.Bool(                         "api",         false,                   "Enable API process.")
	var etcdEndpoints = flag.String(   "etcd",        "http://127.0.0.1:2379", "API : Etcd addresses (comma-delimited)")
	var cassEndpoints = flag.String(   "cassandra",   "127.0.0.1",             "API : Cassandra addresses (comma-delimited)")
	flag.StringVar(&(c.BindIp),        "bindip",      "127.0.0.1",             "API : Address to bind to.")
	flag.StringVar(&(c.CassKeyspace),  "keyspace",    "chan",                  "API : Cassandra keyspace")
	flag.StringVar(&(c.ClusterName),   "clustername", "streamingchan",         "API : Cluster name")
	flag.IntVar(   &(c.HttpPort),      "httpport",    4000,                    "API : Host for HTTP Server for serving stats. 0 for disabled.")
	flag.Parse()

	c.EtcdEndpoints = strings.Split(*etcdEndpoints, ",")
	c.CassEndpoints = strings.Split(*cassEndpoints, ",")
	c.CmdLine = strings.Join(os.Args, " ")
	return c
}

func NewApiServer(stop chan<- bool) *ApiServer {
	as := new(ApiServer)
	as.Config = parseFlags()
	as.stop = stop
	as.Stats = node.NewNodeStats()
	as.Storage = fourchan.NewStorage(as.Config.CassKeyspace, as.Config.CassEndpoints...)
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
	as.Keys = etcd.NewKeysAPI(c)
	return as
}

func (as *ApiServer) Shutdown() {
	log.Print("Recieved command to shutdown. Shutting down in 2 seconds.")
	time.Sleep(2 * time.Second)
	as.stop <- true
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
		go as.Shutdown()
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

func (as *ApiServer) channelHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")

	sort := "ASC"
	web := true
	for k, _ := range r.URL.Query() {
		switch k {
		case "desc":
			sort = "DESC"
		case "json":
			web = false
		}
	}

	pathTokens := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	switch len(pathTokens) {
	// path /{chan}
	case 1:
		w.Header().Set("Content-Type", "application/json")
		boards := as.Storage.GetBoards(pathTokens[0], sort)
		log.Printf("serving %d boards", len(boards))
		j, _ := json.Marshal(boards)
		if _, err := fmt.Fprint(w, string(j)); err != nil {
			log.Print(err)
		}
	// path /{chan}/{board}
	case 2:
		w.Header().Set("Content-Type", "application/json")
		threads := as.Storage.GetThreads(pathTokens[0], pathTokens[1], sort)
		log.Printf("serving board %s with %d threads", pathTokens[1], len(threads))
		j, _ := json.Marshal(threads)
		if _, err := fmt.Fprint(w, string(j)); err != nil {
			log.Print(err)
		}
	// path /{chan}/{board}/{thread}
	case 3:
		if threadNo, e := strconv.Atoi(pathTokens[2]); e == nil {
			postNos := as.Storage.GetPostNumbers(pathTokens[0], pathTokens[1], threadNo)
			posts := as.Storage.GetPosts(pathTokens[0], pathTokens[1], postNos)
			if len(posts) > 0 && posts[0].No == threadNo {
				posts[0].Op = true
			}
			thread := &fourchan.Thread{Posts: posts, No: threadNo}

			log.Printf("serving board %s, thread %d with %d posts", pathTokens[1], threadNo, len(postNos))
			if (web) {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
		    err := t.Execute(w, thread)
		    if err != nil {
		        http.Error(w, err.Error(), http.StatusInternalServerError)
		    }
			} else {
				w.Header().Set("Content-Type", "application/json")
				j, _ := json.Marshal(posts)
				if _, err := fmt.Fprint(w, string(j)); err != nil {
					log.Print(err)
				}
			}
		} else {
			log.Print(e)
		}
	}
}

func (as *ApiServer) hashFileHandler(w http.ResponseWriter, r *http.Request) {
	filename := r.URL.Path[5:]
	fileparts := strings.Split(filename, ".")
	file := as.Storage.GetFileByHash(fileparts[0])
	as.writeFile(w, file)
}

func (as *ApiServer) timeFileHandler(w http.ResponseWriter, r *http.Request) {
	filename := r.URL.Path[6:]
	fileparts := strings.Split(filename, ".")
	if time, err := strconv.ParseInt(fileparts[0], 10, 64); err == nil {
		file := as.Storage.GetFileByTime(time)
		as.writeFile(w, file)
	} else {
		log.Print("invalid file: ", err)
	}
}

func (as *ApiServer) writeFile(w http.ResponseWriter, f *fourchan.File) {
	if len(f.Data) == 0 {
		w.WriteHeader(http.StatusNotFound)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		io.WriteString(w, "Not Found")
	}

	switch f.Ext {
	case ".jpg":
		w.Header().Set("Content-Type", "image/jpeg")
	case ".jpeg":
		w.Header().Set("Content-Type", "image/jpeg")
	case ".png":
		w.Header().Set("Content-Type", "image/png")
	case ".gif":
		w.Header().Set("Content-Type", "image/gif")
	case ".webm":
		w.Header().Set("Content-Type", "video/webm")
	default:
		w.Header().Set("Content-Type", "image/"+f.Ext)
		log.Print("unknown file type: ", f.Ext)
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(f.Data)))
	if _, err := bytes.NewReader(f.Data).WriteTo(w); err != nil {
		log.Print("file write error: ", err)
	}
}

func (as *ApiServer) Serve() error {
	log.Println("Starting HTTP Server on port", as.Config.HttpPort)
	http.HandleFunc("/status/",     as.statusHandler)
	http.HandleFunc("/commands/",   as.commandHandler)
	http.HandleFunc("/4/",          as.channelHandler)
	http.HandleFunc("/md5/",        as.hashFileHandler)
	http.HandleFunc("/file/",       as.timeFileHandler)
	if e := http.ListenAndServe(fmt.Sprintf(":%d", as.Config.HttpPort), nil); e != nil {
		log.Print("Error starting server", e)
		return e
	}
	return nil
}
