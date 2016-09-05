package node

import (
  "log"
  "github.com/llparse/streamingchan/fourchan"
)

const (
  numThreadRoutines = 2
)

func (n *Node) startThreadRoutines() {
  log.Printf("Starting %d thread routines", numThreadRoutines)
  for i := 0; i < 2; i++ {
    go n.threadProcessor(n.CThread, n.CPost)
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
