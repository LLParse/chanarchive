package node

import (
	"encoding/json"
	"fmt"
	"github.com/llparse/streamingchan/version"
	"log"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"
)

func (n *Node) statusHandler(w http.ResponseWriter, r *http.Request) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	stats := map[string]interface{}{
		"board_requests": map[string]interface{}{
			"5min": n.Stats.Aggregate(METRIC_BOARD_REQUESTS, TIME_5SEC, 60),
			"1hr":  n.Stats.Aggregate(METRIC_BOARD_REQUESTS, TIME_1MIN, 60),
			"1d":   n.Stats.Aggregate(METRIC_BOARD_REQUESTS, TIME_1HOUR, 24),
		},
		"threads_processed": map[string]interface{}{
			"5min": n.Stats.Aggregate(METRIC_THREADS, TIME_5SEC, 60),
			"1hr":  n.Stats.Aggregate(METRIC_THREADS, TIME_1MIN, 60),
			"1d":   n.Stats.Aggregate(METRIC_THREADS, TIME_1HOUR, 24),
		},
		"posts_published": map[string]interface{}{
			"5min": n.Stats.Aggregate(METRIC_POSTS, TIME_5SEC, 60),
			"1hr":  n.Stats.Aggregate(METRIC_POSTS, TIME_1MIN, 60),
			"1d":   n.Stats.Aggregate(METRIC_POSTS, TIME_1HOUR, 24),
		},
	}
	data := map[string]interface{}{
		"ok":                1,
		"boards":            n.OwnedBoards,
		"revision":          version.GitHash,
		"build_date":        version.BuildDate,
		"cmd_line":          n.Config.CmdLine,
		"runtime":           time.Since(n.Stats.StartTime),
		"memory":            memStats.Alloc,
		"processId":         os.Getpid(),
		"hostname":          n.Config.Hostname,
		"board_requests":    n.Stats.Lifetime.BoardRequests,
		"threads_processed": n.Stats.Lifetime.Threads,
		"posts_published":   n.Stats.Lifetime.Posts,
		"stats":             stats,
		"nodeidx":           n.LastNodeIdx,
		"nodecount":         n.NodeCount,
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	data["runtime"] = (data["runtime"].(time.Duration)).Seconds()
	out, err := json.Marshal(data)
	if err == nil {
		fmt.Fprint(w, string(out))
	}
}

func (n *Node) commandHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	if strings.Contains(r.URL.Path, "/commands/stop") {
		go func() { n.stop <- true }()
		p := map[string]interface{}{
			"ok":      1,
			"message": "Stop command sent, check logs",
		}
		data, _ := json.Marshal(p)
		fmt.Fprint(w, string(data))
		return
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

func (n *Node) statusServer() {
	if n.Config.HttpPort != 0 {
		log.Println("Starting HTTP status server on port", n.Config.HttpPort)
		http.HandleFunc("/status/", n.statusHandler)
		http.HandleFunc("/commands/", n.commandHandler)
		http.ListenAndServe(fmt.Sprintf(":%d", n.Config.HttpPort), nil)
	}
}
