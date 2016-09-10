package node

import (
  "encoding/json"
  "fmt"
  "log"
  "time"
  "github.com/llparse/streamingchan/fourchan"
)

const (
  numThreadRoutines = 2
)

func (n *Node) startThreadRoutines() {
  log.Printf("Starting %d thread routines", numThreadRoutines)
  for i := 0; i < numThreadRoutines; i++ {
    go n.threadProcessor()
  } 
}

func (n *Node) threadProcessor() {
  for {
    select {
    case thread := <-n.CThread:
      log.Printf("processing /%s/thread/%d", thread.Board, thread.No)
      n.Storage.PersistThread(thread)
      if thread, err := DownloadThread(thread.Board, thread.No); err == nil {
        n.Stats.Incr(METRIC_THREADS, 1)
        var postNos []int
        for _, post := range thread.Posts {
          // TODO iff post.Time >= thread.LM
          postNos = append(postNos, post.No)
          n.CPost <- post
          n.Stats.Incr(METRIC_POSTS, 1)
        }
        n.Storage.PersistThreadPosts(&thread, postNos)
      } else {
        log.Print("Error downloading thread: ", err)
      }
    case <-n.stop:
      return
    }
  }
}

func DownloadThread(board string, thread int) (fourchan.Thread, error) {
  url := fmt.Sprintf("http://api.4chan.org/%s/res/%d.json", board, thread)
  if data, _, _, _, err := fourchan.EasyGet(url, time.Time{}); err == nil {
    var t fourchan.Thread
    if err := json.Unmarshal(data, &t); err == nil {
      t.No = thread
      for idx, _ := range t.Posts {
        t.Posts[idx].Board = board
      }
      return t, nil
    } else {
      return t, err
    }
  } else {
    return fourchan.Thread{}, err
  }
}
