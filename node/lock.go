package node

import (
  "log"
  "time"
  "golang.org/x/net/context"
  etcd "github.com/coreos/etcd/client"
)

type EtcdLock struct {
  keys   etcd.KeysAPI
  path   string
  owner  string
  ttl    time.Duration
  ticker *time.Ticker
  stop   chan bool
}

func (n *Node) NewEtcdLock(path string, owner string, timeout time.Duration) *EtcdLock {
  return &EtcdLock{
    keys: n.Keys,
    path: path,
    owner: owner,
    ttl: timeout,
    stop: make(chan bool),
  }
}


func (l *EtcdLock) Acquire() error {
  _, err := l.keys.Set(
    context.Background(),
    l.path,
    l.owner,
    &etcd.SetOptions{TTL: l.ttl, PrevExist: etcd.PrevNoExist})

  if err2, ok := err.(etcd.Error); ok && err2.Code == etcd.ErrorCodeNodeExist {
    log.Print("lock already held: ", l.path)
  }
  if err == nil {
    log.Print("lock acquired: ", l.path)
    l.ticker = time.NewTicker(l.ttl / 2)
    go l.refresh()
  }

  return err
}

func (l *EtcdLock) refresh() {
  for {
    select {
    case <-l.ticker.C:
      _, err := l.keys.Set(
        context.Background(),
        l.path,
        "",
        &etcd.SetOptions{
          TTL: l.ttl,
          PrevValue: l.owner,
          Refresh: true,
        })
      if err != nil {
        log.Print("lock NOT refreshed: ", l.path)
        log.Print(err)
      } else {
        log.Print("lock refreshed: ", l.path)
      }
    case <-l.stop:
      return
    }
  }
}

func (l *EtcdLock) Release() {
  l.stop <- true
  if _, err := l.keys.Delete(context.Background(), l.path, nil); err != nil {
    log.Printf("lock NOT released: ", l.path)
    log.Println(err)
  } else {
    log.Print("lock released: ", l.path)
  }
}
