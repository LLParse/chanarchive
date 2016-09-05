package node

import (
  "encoding/json"
  "fmt"
  "log"
  "net/http"
  "time"
  etcd "github.com/coreos/etcd/client"
  "github.com/llparse/streamingchan/fourchan"
  "golang.org/x/net/context"
)

const (
  boardPath = "/%s/boards/%s"
  boardLockPath = "/%s/board-lock/%s"
  boardLastModifiedPath = "/%s/board-lm/%s"
  boardLockTTL = 10 * time.Second
  boardRefreshPeriod = 5 * time.Second
)

func (n *Node) downloadBoards() {
  log.Print("Downloading boards list...")
  var err error
  if n.Boards, err = fourchan.DownloadBoards(n.Config.OnlyBoards, n.Config.ExcludeBoards); err != nil {
    log.Fatal(err)
  }
}

func (n *Node) startBoardRoutines() {
  numBoards := len(n.Boards.Boards)

  if numBoards == 0 {
    log.Fatal("No boards to process")
  } else {
    log.Printf("Starting %d board routines", numBoards)
  }

  go func() {
    boardIndex := 0
    ticker := time.NewTicker(boardRefreshPeriod / time.Duration(numBoards))
    for _ = range ticker.C {
      board := n.Boards.Boards[boardIndex]
      
      go n.boardProcessor(board, n.CThread)
      n.Storage.PersistBoard(board)

      if boardIndex + 1 == numBoards {
        ticker.Stop()
        break
      } else {
        boardIndex += 1
      }
    }   
  }()
}

func (n *Node) boardProcessor(board *fourchan.Board, threadChan chan<- *fourchan.Thread) {
  boardTicker := time.NewTicker(boardRefreshPeriod)
  for _ = range boardTicker.C {
    if err := n.acquireBoardLock(board.Board); err != nil {
      continue
    }
    lockTicker := time.NewTicker(boardLockTTL / 2)
    go func() {
      for _ = range lockTicker.C {
        if err := n.refreshBoardLock(board.Board); err != nil {
          log.Fatal("couldn't refresh lock on board ", board.Board, ": ", err)
        }
      }
    }()
    log.Printf("processing board %s", board.Board)
    board.LM = n.getBoardLastModified(board.Board)
    if threads, statusCode, lastModifiedStr, e := n.DownloadBoard(board.Board, board.LM); e == nil {
      n.Stats.Incr(METRIC_BOARD_REQUESTS, 1)
      board.LM, _ = time.Parse(http.TimeFormat, lastModifiedStr)
      for _, thread := range threads {
        threadChan <- thread
      }
      n.setBoardLastModified(board.Board, board.LM)
    } else if statusCode != 304 {
      log.Print("Error downloading board ", board.Board, " ", e)
    } else {
      //log.Print("Board ", board.Board, " not modified")
    }
    // TODO use a chan and release on cleanup
    lockTicker.Stop()
    n.releaseBoardLock(board.Board)
  }
}

func (n *Node) DownloadBoard(board string, lastModified time.Time) ([]*fourchan.Thread, int, string, error) {
  if data, _, statusCode, lastModified, err := fourchan.EasyGet(
    fmt.Sprintf("http://api.4chan.org/%s/threads.json", board), lastModified); err == nil {

    var bp []*fourchan.BoardPage
    if err := json.Unmarshal(data, &bp); err != nil {
      return nil, statusCode, lastModified, err      
    }

    n.setBoard(board, data)

    var t []*fourchan.Thread
    for _, page := range bp {
      for _, thread := range page.Threads {
        thread.Board = board
        t = append(t, thread)
      }
    }
    return t, statusCode, lastModified, nil
  } else {
    return nil, 500, "", err
  }
}

func (n *Node) setBoard(board string, data []byte) {
  _, err := n.Keys.Set(
    context.Background(),
    fmt.Sprintf(boardPath, n.Config.ClusterName, board),
    string(data),
    nil)
  if err != nil {
    log.Fatal(err)
  }
}

func (n *Node) acquireBoardLock(board string) error {
  _, err := n.Keys.Set(
    context.Background(),
    fmt.Sprintf(boardLockPath, n.Config.ClusterName, board),
    n.Id,
    &etcd.SetOptions{TTL: boardLockTTL, PrevExist: etcd.PrevNoExist})

  if err2, ok := err.(etcd.Error); ok && err2.Code == etcd.ErrorCodeNodeExist {
    if n.Config.Verbose {
      log.Printf("lock already held for board %s", board)
    }
  } else if err != nil {
    log.Println(err)
  }

  return err
}

func (n *Node) refreshBoardLock(board string) error {
  // can't do a refresh with CaS with 2.3.7 (yet), so refreshless it is...
  // https://github.com/coreos/etcd/issues/5651
  _, err := n.Keys.Set(
    context.Background(),
    fmt.Sprintf(boardLockPath, n.Config.ClusterName, board),
    n.Id,
    &etcd.SetOptions{TTL: boardLockTTL, PrevValue: n.Id})
  return err
}

func (n *Node) releaseBoardLock(board string) {
  path := fmt.Sprintf(boardLockPath, n.Config.ClusterName, board)
  if _, err := n.Keys.Delete(context.Background(), path, nil); err != nil {
    log.Printf("couldn't release lock on board %s", board)
    log.Println(err)
  }
}

func (n *Node) getBoardLastModified(board string) time.Time {
  var resp *etcd.Response
  var err error
  lm := time.Unix(0, 0)

  path := fmt.Sprintf(boardLastModifiedPath, n.Config.ClusterName, board)
  resp, err = n.Keys.Get(context.Background(), path, nil)
  if err != nil {
    if err2, ok := err.(etcd.Error); ok && err2.Code == etcd.ErrorCodeKeyNotFound {
      log.Printf("no lastModified set for board %s\n", board)     
    } else {
      log.Fatal("error getting lastModified: ", err)
    }
  } else {
    lm, err = time.Parse(http.TimeFormat, resp.Node.Value)
    if err != nil {
      log.Println("error parsing lastModified: ", err)
    }
  }
  return lm
}

func (n *Node) setBoardLastModified(board string, lastModified time.Time) {
  if _, err := n.Keys.Set(
      context.Background(),
      fmt.Sprintf(boardLastModifiedPath, n.Config.ClusterName, board),
      lastModified.Format(http.TimeFormat),
      nil); err != nil {
    log.Printf("Error setting lastModified for board %s", board)
  }
}
