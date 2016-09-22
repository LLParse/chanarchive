package node

import (
  "encoding/json"
  "fmt"
  "log"
  "sync"
  "time"
  "github.com/llparse/streamingchan/fourchan"
)

func (n *Node) startThreadRoutines(processors *sync.WaitGroup) {
  defer processors.Done()

  log.Printf("Starting %d thread routines", numThreadRoutines)
  var wg sync.WaitGroup
  wg.Add(numThreadRoutines)
  for i := 0; i < numThreadRoutines; i++ {
    go n.threadProcessor(&wg)
  }
  wg.Wait()
  log.Print("Thread routines finished, closing chan.")
  close(n.CThread)
  n.stopPost <- true
}

func (n *Node) threadProcessor(wg *sync.WaitGroup) {
  defer wg.Done()
  for {
    select {
    case thread := <-n.CThread:
      //log.Printf("processing /%s/thread/%d", thread.Board, thread.No)
      n.Storage.PersistThread(thread)
      if t, err := DownloadThread(thread.Board, thread.No); err == nil {
        n.Stats.Incr(METRIC_THREADS, 1)
        var postNos []int
        for _, post := range t.Posts {
          // TODO iff post.Time >= thread.LM
          postNos = append(postNos, post.No)
          n.CPost <- post
          n.Stats.Incr(METRIC_POSTS, 1)
        }
        n.Storage.PersistThreadPosts(t, postNos)
      } else {
        log.Print("Error downloading thread: ", err)
      }
    case <-n.stopThread:
      n.stopThread <- true
      //log.Print("Thread routine stopped")
      return
    }
  }
}

func DownloadThread(board string, thread int) (*fourchan.Thread, error) {
  url := fmt.Sprintf("http://api.4chan.org/%s/res/%d.json", board, thread)
  if data, _, _, _, err := fourchan.EasyGet(url, time.Time{}); err == nil {
    var t fourchan.Thread
    if err := json.Unmarshal(data, &t); err == nil {
      t.Board = board
      t.No = thread
      for idx, _ := range t.Posts {
        t.Posts[idx].Board = board
      }
      return &t, nil
    } else {
      return &t, err
    }
  } else {
    return &fourchan.Thread{}, err
  }
}
