package node

import (
  "log"
  "github.com/llparse/streamingchan/fourchan"
)

const (
  numFileRoutines = 2
)

func (n *Node) startFileRoutines() {
  log.Printf("Starting %d file routines", numFileRoutines)
  for i := 0; i < 2; i++ {
    go n.fileProcessor(n.CFile)
  }
}

func (n *Node) fileProcessor(files <-chan *fourchan.File) {
  for file := range files {
    if n.Config.NoFiles {
      continue
    }
    //log.Printf("processing /%s/file/%d", file.Board, file.Tim)
    if !n.Storage.FileExists(file) {
      data, err := fourchan.DownloadFile(file.Board, file.Tim, file.Ext)
      if err == nil {
        file.Data = data
        n.Storage.WriteFile(file)
      } else {
        log.Printf("Error downloading file %+v: %+v", file, err)
      }
    } else if n.Config.Verbose {
      log.Printf("File exists: %+v", file)
    }
  }
}
