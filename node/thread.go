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

func (n *Node) threadProcessor(threads <-chan *fourchan.Thread, posts chan<- *fourchan.Post) {
  for thread := range threads {
    //log.Printf("processing /%s/thread/%d", thread.Board, thread.No)
    n.Storage.PersistThread(thread)
    if thread, err := fourchan.DownloadThread(thread.Board, thread.No); err == nil {
      n.Stats.Incr(METRIC_THREADS, 1)
      var postNos []int
      for _, post := range thread.Posts {
        // TODO iff post.Time >= thread.LM
        postNos = append(postNos, post.No)
        posts <- post
        n.Stats.Incr(METRIC_POSTS, 1)
      }
      n.Storage.PersistThreadPosts(thread, postNos)
    } else {
      //log.Print("Error downloading thread: ", err)
    }
  }
}
