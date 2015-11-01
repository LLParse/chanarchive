#!/bin/bash -E

# Install prerequisites
# TODO: oracle instead
sudo apt-get install -y openjdk-7-jre git

# Install Cassandra
curl -L http://debian.datastax.com/debian/repo_key | sudo apt-key add -
sudo sh -c 'echo "deb http://debian.datastax.com/community/ stable main" > /etc/apt/sources.list.d/datastax.list'
sudo apt-get update
sudo apt-get install -y cassandra=2.2.3
sudo service cassandra start

# Install Golang
curl -LO https://storage.googleapis.com/golang/go1.5.1.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.5.1.linux-amd64.tar.gz
rm go1.5.1.linux-amd64.tar.gz
echo export PATH="$PATH:$HOME/go/bin:/usr/local/go/bin" >> ~/.profile 
echo export GOROOT=/usr/local/go >> ~/.profile
echo export GOPATH=$HOME/go >> ~/.profile
mkdir -p $HOME/go
export PATH="$PATH:$HOME/go/bin:/usr/local/go/bin"
export GOROOT=/usr/local/go
export GOPATH=$HOME/go

# Install etcd & etcdctl
go get github.com/coreos/etcd
go get github.com/coreos/etcdctl
sudo cp $GOPATH/bin/etcd /usr/local/bin
sudo cp $GOPATH/bin/etcdctl /usr/local/bin
sudo cp $GOPATH/src/github.com/llparse/streamingchan/provision/*.conf /etc/init
sudo service etcd start

# Install streamingchan and create schema
go get github.com/llparse/streamingchan
sudo cp $GOPATH/bin/streamingchan /usr/local/bin
cqlsh -f $GOPATH/src/github.com/llparse/streamingchan/model.cql
sudo service chan-api start
sudo service chan-node start