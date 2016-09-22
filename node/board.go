package node

import (
  "encoding/json"
  "fmt"
  "log"
  "net/http"
  "sync"
  "time"
  etcd "github.com/coreos/etcd/client"
  "github.com/llparse/streamingchan/fourchan"
  "golang.org/x/net/context"
)

const (
  boardStatePath = "/%s/boards/%s/state"
  boardLockPath = "/%s/boards/%s/lock"
  boardLastModifiedPath = "/%s/boards/%s/lm"
)

func (n *Node) downloadBoards() {
  log.Print("Downloading boards list...")
  var err error
  if n.Boards, err = fourchan.DownloadBoards(n.Config.OnlyBoards, n.Config.ExcludeBoards); err != nil {
    log.Fatal(err)
  }
}

func (n *Node) startBoardRoutines(processors *sync.WaitGroup) {
  defer processors.Done()

  numBoards := len(n.Boards.Boards)

  if numBoards == 0 {
    log.Fatal("No boards to process")
  } else {
    log.Printf("Starting %d board routines", numBoardRoutines)
  }

  go func () {
    for _, board := range n.Boards.Boards {
      n.Storage.PersistBoard(board)
    }
  }()

  go func() {
    boardIndex := 0
    for {
      //log.Print("Wrote board ", n.Boards.Boards[boardIndex].Board, " to chan")
      select {
      case n.CBoard <- n.Boards.Boards[boardIndex]:
        boardIndex += 1
        boardIndex %= numBoards
      case <-n.stop:
        log.Print("Stopped marking boards for processing.")
        for ; len(n.CBoard) > 0; {
          <-n.CBoard
        }
        n.stopBoard <- true
        return
      default:
        time.Sleep(1 * time.Second)
      }
    }
  }()

  var wg sync.WaitGroup
  wg.Add(numBoardRoutines)
  for i := 0; i < numBoardRoutines; i++ {
    go n.boardProcessor(&wg)
  }
  wg.Wait()
  log.Print("Board routines finished, closing chan.")
  close(n.CBoard)
  n.stopThread <- true
}

func (n *Node) boardProcessor(wg *sync.WaitGroup) {
  defer wg.Done()
  for {
    select {
    case board := <-n.CBoard:
      boardLock := n.NewEtcdLock(
        fmt.Sprintf(boardLockPath, n.Config.ClusterName, board.Board),
        n.Id,
        boardLockTTL)

      if err := boardLock.Acquire(); err != nil {
        continue
      }
      log.Printf("processing board %s", board.Board)
      board.LM = n.getBoardLastModified(board.Board)
      if threads, statusCode, lastModifiedStr, e := n.DownloadBoard(board.Board, board.LM); e == nil {
        n.Stats.Incr(METRIC_BOARD_REQUESTS, 1)
        board.LM, _ = time.Parse(http.TimeFormat, lastModifiedStr)
        for _, thread := range threads {
          n.CThread <- thread
        }
        n.setBoardLastModified(board.Board, board.LM)
      } else if statusCode != 304 {
        log.Print("Error downloading board ", board.Board, " ", e)
      } else {
        //log.Print("Board ", board.Board, " not modified")
      }
      // FIXME waitGroup for threads to complete 
      boardLock.Release()

    case <-n.stopBoard:
      n.stopBoard <- true
      //log.Print("Board routine stopped")
      return
    }
  }
}

func (n *Node) DownloadBoard(board string, lastModified time.Time) ([]*fourchan.Thread, int, string, error) {
  if data, _, statusCode, lastModified, err := fourchan.EasyGet(
    fmt.Sprintf("http://api.4chan.org/%s/threads.json", board), lastModified); err == nil {

    var bp []*fourchan.BoardPage
    if err := json.Unmarshal(data, &bp); err != nil {
      return nil, statusCode, lastModified, err      
    }

    if statusCode != 304 {
      n.setBoardState(board, string(data))
    }

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

func (n *Node) setBoardState(board string, state string) {
  _, err := n.Keys.Set(
    context.Background(),
    fmt.Sprintf(boardStatePath, n.Config.ClusterName, board),
    string(state),
    nil)
  if err != nil {
    log.Fatal(err)
  }
}

func (n *Node) getBoardLastModified(board string) time.Time {
  //log.Print("get board ", board, " last modified")
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
  //log.Print("set board ", board, " last modified ", lastModified)
  if _, err := n.Keys.Set(
      context.Background(),
      fmt.Sprintf(boardLastModifiedPath, n.Config.ClusterName, board),
      lastModified.Format(http.TimeFormat),
      nil); err != nil {
    log.Printf("Error setting lastModified for board %s", board)
  }
}
