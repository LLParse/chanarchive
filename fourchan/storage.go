package fourchan

import (
  "log"
  "github.com/gocql/gocql"
  "time"
)

type Storage struct {
  config  *gocql.ClusterConfig
  session *gocql.Session
}

func NewStorage(keyspace string, hosts ...string) *Storage {
  storage := new(Storage)
  storage.config = gocql.NewCluster(hosts...)
  storage.config.Keyspace = keyspace
  storage.config.Consistency = gocql.Quorum
  storage.session = storage.NewSession()
  return storage
}

func (s *Storage) Close() {
  s.session.Close()
}

func (s *Storage) NewSession() *gocql.Session {
  delay := 500 * time.Millisecond
  for i := 0; i < 5; i++ {
    session, err := gocql.NewSession(*s.config)
    if err != nil {
      log.Print(err)
      time.Sleep(delay)
      delay = delay * 2
      continue
    }
    return session
  }
  log.Fatal("Couldn't create a c* session after 5 tries!")
  return nil
}

func (s *Storage) PersistBoard(b *Board) {
  if err := s.session.Query(`INSERT INTO board (chan, board, title, worksafe, perpage, pages) VALUES (?, ?, ?, ?, ?, ?)`,
    "4", b.Board, b.Title, b.WsBoard, b.PerPage, b.Pages).Exec(); err != nil {
    log.Print("Persist board error: ", err)
  }
}

func (s *Storage) PersistThread(t *ThreadInfo) {
  if err := s.session.Query(`INSERT INTO thread (chan, board, number) VALUES (?, ?, ?)`,
    "4", t.Board, t.No).Exec(); err != nil {
    log.Print("Persist thread error: ", err)
  }
}

func (s *Storage) PersistPost(p *Post) {
  if err := s.session.Query(`INSERT INTO post (chan, board, number, resto, sticky, closed, now, time, name, trip, id, capcode, country, countryName, sub, com, tim, filename, ext, fsize, md5, w, h, tnw, tnh, fileDeleted, spoiler, customSpoiler, omittedPosts, omittedImages, replies, images, bumpLimit, imageLimit, lastModified) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    "4", p.Board, p.No, p.Resto, p.Sticky, p.Closed, p.Now, p.Time, p.Name, p.Trip, p.Id, p.Capcode, p.Country, p.CountryName, 
    p.Sub, p.Com, p.Tim, p.Filename, p.Ext, p.FSize, p.Md5, p.W, p.H, p.TnW, p.TnH, p.FileDeleted, p.Spoiler, p.CustomSpoiler, 
    p.OmittedPosts, p.OmittedImages, p.Replies, p.Images, p.BumpLimit, p.ImageLimit, p.LastModified).Exec(); err != nil {
    log.Print("Persist post error: ", err)
  }
}

func (s *Storage) PersistFile(f *File) {
  if err := s.session.Query(`INSERT INTO file (md5, ext, fsize, data) VALUES (?, ?, ?, ?)`,
    f.Md5, f.Ext, f.FSize, f.Data).Exec(); err != nil {
    log.Print("Persist file error: ", err)
  }
}

func (s *Storage) FileExists(f *File) bool {
  var value string
  if err := s.session.Query(`SELECT md5 FROM file WHERE md5 = ?`, f.Md5).Scan(&value); err != nil {
    return false
  } else if value != f.Md5 {
    log.Printf("md5: %s, got %s", f.Md5, value)
    return false
  }
  return true
}

func (s *Storage) GetBoards(channel string) []*Board {
  iter := s.session.Query(`SELECT board, title, worksafe, pages, perpage FROM board WHERE chan = ?`, channel).Iter()
  var boards []*Board
  board := &Board{}
  for iter.Scan(&board.Board, &board.Title, &board.WsBoard, &board.Pages, &board.PerPage) {
    boards = append(boards, board)
  }
  return boards
}

func (s *Storage) GetThreads(channel string, board string) []*ThreadInfo {
  var threads []*ThreadInfo
  iter := s.session.Query(`SELECT number FROM thread WHERE chan = ? AND board = ?`, channel, board).Iter()
  for {
    thread := &ThreadInfo{}
    if !iter.Scan(&thread.No) {
      break
    }
    thread.Board = board
    threads = append(threads, thread)
  }
  return threads
}
