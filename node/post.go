package node

import (
  "log"
  "github.com/llparse/streamingchan/fourchan"
)

const (
  numPostRoutines = 4
)

func (n *Node) startPostRoutines() {
  log.Printf("Starting %d post routines", numPostRoutines)
  for i := 0; i < 4; i++ {
    go n.postProcessor(n.CPost, n.CFile)
  }
}

func (n *Node) postProcessor(posts <-chan *fourchan.Post, files chan<- *fourchan.File) {
  for post := range posts {
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
      files <- file
    }
  }
}
