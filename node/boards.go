package node

import (
	"fmt"
  "github.com/coreos/etcd/Godeps/_workspace/src/golang.org/x/net/context"
  etcd "github.com/coreos/etcd/client"
	"github.com/llparse/streamingchan/fourchan"
	"log"
	"net/http"
	"strconv"
	"time"
)

func (n *Node) checkBoardLock(board string, stop <-chan bool) bool {
	resp, err := n.EtcKeys.Get(context.Background(), n.Config.Cluster + "/boards-owner/" + board, nil)
	if err != nil {
		log.Print("Failed to connect to etcd: ", err)
		return true
	}
	if resp.Node.Value == n.NodeId || resp.Node.Value == "" || resp.Node.Value == "none" {
		return true
	}
	log.Printf("Board %s is locked (by %s). Trying to unlock.", board, resp.Node.Value)
	defer log.Printf("Done waiting for %s.", board)
	timeout := make(chan bool, 1)
	for tries := 0; tries < 5; tries++ {
		resp, err := n.EtcKeys.Get(context.Background(), n.Config.Cluster + "/boards-owner/" + board, nil)
		if err == nil && (resp.Node.Value == n.NodeId || resp.Node.Value == "") {
			return true
		}
		go func() {
			time.Sleep(time.Duration(tries) * time.Second)
			timeout <- true
		}()
		select {
		case <-stop:
			return false
		case <-timeout:
			continue
		}
	}
	return true
}

func (n *Node) ProcessBoard(nodeIds []string, board string, stop <-chan bool) {
	defer n.RoutineWaitGroup.Done()
	n.RoutineWaitGroup.Add(1)
	if !n.checkBoardLock(board, stop) {
		return
	}
	message := 0
	resp, err := n.EtcKeys.Get(context.Background(), n.Config.Cluster + "/boards/" + board, nil)
	if err != nil && (err.(etcd.Error)).Code != 100 {
		log.Print("Failed to connect to etcd: ", err)
		return
	}
	lastModified := 0
	var lastModifiedHeader time.Time
	if err == nil {
		if lastModified, err = strconv.Atoi(resp.Node.Value); err != nil {
			lastModified = 0
		}
	}
	multiplier := 1
	maxMod := 0
	timeout := make(chan bool, 1)
	resp, err = n.EtcKeys.Set(context.Background(), n.Config.Cluster+"/boards-owner/"+board, n.NodeId, nil)
	if err != nil && (err.(etcd.Error)).Code != 100 {
		log.Print("Failed to connect to etcd: ", err)
		return
	}
	for {
		oldLM := lastModified
		if threads, statusCode, lastModifiedStr, e := fourchan.DownloadBoard(board, lastModifiedHeader); e == nil {
			lastModifiedHeader, _ = time.Parse(http.TimeFormat, lastModifiedStr)
			log.Print("LM : ", lastModified)
			for _, page := range threads {
				for _, thread := range page.Threads {
					if thread.LastModified > lastModified && lastModified != 0 {
						//var ti fourchan.ThreadInfo
						//ti = thread // copy
						//ti.Board = page.Board
						//ti.LastModified = lastModified
						thread.Board = page.Board
						thread.MinPost = lastModified
						thread.OwnerId = nodeIds[(message % len(nodeIds))]
						message++
						if n.ThreadPub == nil {
							return
						}
						//log.Printf("Sending %d to %s", thread.No, thread.OwnerId)
						n.ThreadPub <- &thread
						multiplier = 1
					}
					if thread.LastModified > maxMod {
						maxMod = thread.LastModified
					}
				}
			}
			lastModified = maxMod
		} else if statusCode != 304 {
			log.Print("Error downloading board ", board, " ", e)
		}
		if oldLM != lastModified {
			n.EtcKeys.Set(context.Background(), n.Config.Cluster+"/boards/"+board, fmt.Sprintf("%d", lastModified), nil)
		}
		n.Stats.Incr(METRIC_BOARD_REQUESTS, 1)
		go func() {
			//log.Printf("Waiting %dms", 500 * multiplier)
			time.Sleep(time.Duration(multiplier) * 500 * time.Millisecond)
			timeout <- true
		}()
		select {
		case <-stop:
			return
		case <-timeout:
			multiplier *= 2
		}
		if multiplier > 8 {
			multiplier = 8
		}
	}
}
