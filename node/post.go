package node

import (
  "log"
  "sync"
  "github.com/llparse/streamingchan/fourchan"
)

func (n *Node) startPostRoutines(processors *sync.WaitGroup) {
  defer processors.Done()

  log.Printf("Starting %d post routines", numPostRoutines)
  var wg sync.WaitGroup
  wg.Add(numPostRoutines)
  for i := 0; i < numPostRoutines; i++ {
    go n.postProcessor(&wg)
  }
  wg.Wait()
  log.Print("Post routines finished, closing chan.")
  close(n.CPost)
  n.stopFile <- true
}

func (n *Node) postProcessor(wg *sync.WaitGroup) {
  defer wg.Done()
  for {
    select {
    case post := <-n.CPost:
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
        //n.writeFileInfo(*file)
        n.CFile <- file
      }
    case <-n.stopPost:
      n.stopPost <- true
      //log.Print("Post routine stopped")
      return
    }
  }
}
