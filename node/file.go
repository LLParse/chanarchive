package node

import (
  "encoding/json"
  "fmt"
  "log"
  "strconv"
  "strings"
  "github.com/llparse/streamingchan/fourchan"
  etcd "github.com/coreos/etcd/client"
  "golang.org/x/net/context"
)

const (
  fileInfoPath = "/%s/files/%d/info"
  fileLockPath = "/%s/files/%d/lock"
  numFileRoutines = 1
)

func (n *Node) startFileRoutines() {
  n.loadFiles()
  go n.fileWatcher()

  log.Printf("Starting %d file processors", numFileRoutines)
  for i := 0; i < numFileRoutines; i++ {
    go n.fileProcessor()
  }
}

func (n *Node) fileProcessor() {
  for {
    select {
    case file := <-n.CFile:
      // TODO shouldn't even start processor/publish to chan
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
    case <-n.stop:
      return
    }
  }
}

func (n *Node) loadFiles() {
  resp, err := n.Keys.Get(
    context.Background(),
    fmt.Sprintf("/%s/files", n.Config.ClusterName),
    &etcd.GetOptions{Recursive: true})
  if err != nil {
    return
  }
  for _, file := range resp.Node.Nodes {
    n.loadFile(file)
  }
  log.Print("Loaded ", len(n.Files), " files")
}

func (n *Node) loadFile(file *etcd.Node) {
  next := false
  for _, token := range strings.Split(file.Key, "/") {
    if token == "files" {
      next = true
    } else if next {
      key, err := strconv.Atoi(token)
      if err != nil {
        log.Print(err)
      } else {
        n.FileMutex.Lock()
        n.Files[key] = file.Value
        n.FileMutex.Unlock()
      }
      break
    }
  }
}

func (n *Node) fileWatcher() {
  watcher := n.Keys.Watcher(
    fmt.Sprintf("/%s/files", n.Config.ClusterName), 
    &etcd.WatcherOptions{Recursive: true})

  for {
    resp, err := watcher.Next(context.Background())
    if err != nil {
      panic(err)
    }
    if resp.Action == "create" {
      n.loadFile(resp.Node)
    } else if resp.Action == "delete" {
      log.Print("delete: ", resp.Node)
    }
  }
}

func (n *Node) writeFileInfo(file fourchan.File) {
  data, err := json.Marshal(file)
  if err == nil {
    _, err = n.Keys.Set(
      context.Background(),
      fmt.Sprintf(fileInfoPath, n.Config.ClusterName, file.Tim),
      string(data),
      &etcd.SetOptions{PrevExist: etcd.PrevNoExist})
    if err != nil {
      //log.Print(err)
    }
  } else {
    panic(err)
  }
}
