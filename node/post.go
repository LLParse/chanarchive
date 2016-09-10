package node

import (
  "log"
  "github.com/llparse/streamingchan/fourchan"
)

const (
  numPostRoutines = 8
)

func (n *Node) startPostRoutines() {
  log.Printf("Starting %d post routines", numPostRoutines)
  for i := 0; i < numPostRoutines; i++ {
    go n.postProcessor()
  }
}

func (n *Node) postProcessor() {
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
        n.writeFileInfo(*file)
        n.CFile <- file
      }
    case <-n.stop:
      return
    }
  }
}
