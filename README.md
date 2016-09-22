# ChanArchive
September 22, 2016

ChanArchive is a distributed api designed to archive the [4Chan API](https://github.com/4chan/4chan-API). It is built to run on Rancher. This is mostly an experiment to gather 4chan usage statistics, as well as load test the Etcd and Cassandra Rancher templates.

## Overview

The infrastructure comprises four parts:

* [etcd](https://github.com/coreos/etcd), used for distributed locking
* [cassandra](https://github.com/apache/cassandra), a distributed NoSQL database
* ChanArchive node, downloads the content from 4chan
* ChanArchive api, serves the content to end users

### Dependencies

+ [etcd client](https://github.com/coreos/etcd/tree/master/client)
+ [gocql](https://github.com/gocql/gocql)
+ [go-bindata](https://github.com/jteeuwen/go-bindata) (Optional: For development only: `go get -u github.com/jteeuwen/go-bindata/... && go-bindata -o ./asset/bindata.go -pkg asset asset/...`)

### Building

If you have the appropriate dependecies, simply build with `go build`. Alternatively you can run `./version.sh` to embed the build date and git hash into the build.
