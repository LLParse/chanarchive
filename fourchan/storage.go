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

func (s *Storage) PersistThreadPosts(t *ThreadInfo, post []int) {
  if err := s.session.Query(`UPDATE thread SET posts = posts + ? WHERE chan = ? AND board = ? AND number = ?`,
    post, "4", t.Board, t.No).Exec(); err != nil {
    log.Print("Persist thread post error: ", err)
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

func (s *Storage) WriteFile(f *File) {
  if err := s.session.Query(`INSERT INTO file (md5, ext, fsize, data) VALUES (?, ?, ?, ?)`,
    f.Md5, f.Ext, f.FSize, f.Data).Exec(); err != nil {
    log.Print("Persist file error: ", err)
  } else {
    s.writeFileTime(f)
    s.writeFileHash(f)
  }
}

func (s *Storage) writeFileTime(f *File) {
  if err := s.session.Query(`INSERT INTO file_time (time, md5) VALUES (?, ?)`, f.Tim, f.Md5).Exec(); err != nil {
    log.Print("Write file time error: ", err)
  }
}

func (s *Storage) writeFileHash(f *File) {
  if err := s.session.Query(`INSERT INTO file_hash (md5) VALUES (?)`, f.Md5).Exec(); err != nil {
    log.Print("Write file hash error: ", err)
  } else {
    if err = s.session.Query(`UPDATE file_hash SET time = time + ? WHERE md5 = ?`, []int64{f.Tim}, f.Md5).Exec(); err != nil {
      log.Print("Write file hash error: ", err)
    }    
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

type SortOrder struct {

}

func (s *Storage) GetBoards(channel string, sort string) []*Board {
  if sort == "" {
    sort = "ASC"
  }
  iter := s.session.Query(`SELECT board, title, worksafe, pages, perpage FROM board WHERE chan = ? ORDER BY board ` + sort, channel).Iter()
  var boards []*Board
  for {
    board := &Board{}
    if iter.Scan(&board.Board, &board.Title, &board.WsBoard, &board.Pages, &board.PerPage) {
      boards = append(boards, board)
    } else {
      break
    }
  }
  return boards
}

func (s *Storage) GetThreads(channel string, board string, sort string) []*ThreadInfo {
  if sort == "" {
    sort = "ASC"
  }
  iter := s.session.Query(`SELECT number FROM thread WHERE chan = ? AND board = ? ORDER BY number ` + sort, channel, board).Iter()
  var threads []*ThreadInfo
  for {
    thread := &ThreadInfo{}
    if iter.Scan(&thread.No) {
      threads = append(threads, thread)
    } else {
      break
    }
  }
  return threads
}

func (s *Storage) GetPostNumbers(channel string, board string, threadNo int) []int {
  query := s.session.Query(`SELECT posts FROM thread WHERE chan = ? AND board = ? AND number = ?`, channel, board, threadNo)
  var postNos []int
  if err := query.Scan(&postNos); err != nil {
    log.Print("Couldn't get posts for thread ", threadNo)
  }
  return postNos
}

func (s *Storage) GetPosts(channel string, board string, postNos []int) []*Post {
  iter := s.session.Query(`SELECT number, com, tim, filename, ext, fsize, md5 FROM post WHERE chan = ? AND board = ? AND number IN ?`, channel, board, postNos).Iter()
  var posts []*Post
  for {
    post := &Post{}
    if iter.Scan(&post.No, &post.Com, &post.Tim, &post.Filename, &post.Ext, &post.FSize, &post.Md5) {
      posts = append(posts, post)
    } else {
      break
    }
  }
  return posts
}

func (s *Storage) GetFileByHash(md5 string) *File {
  file := &File{}
  query := s.session.Query(`SELECT md5, ext, fsize, data FROM file WHERE md5 = ?`, md5)
  if err := query.Scan(&file.Md5, &file.Ext, &file.FSize, &file.Data); err != nil {
    log.Print("Couldn't get file ", md5, ": ", err)
  }
  return file
}

func (s *Storage) GetFileByTime(time int64) *File {
  var md5 string
  if err := s.session.Query(`SELECT md5 FROM file_time WHERE time = ?`, time).Scan(&md5); err != nil {
    log.Print("Couldn't get md5 for file ", time)
  }
  return s.GetFileByHash(md5)
}
