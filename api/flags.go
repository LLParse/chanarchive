package api

import (
	"flag"
	"os"
)

var Flags *FlagConfig

type FlagConfig struct {
	MaxConnections int
	BindIp         string
	EtcdEndpoints  string
	CassEndpoints  string
	CassKeyspace   string
	ClusterName    string
	HttpPort       int
	Api            bool
}

func init() {
	for _, arg := range os.Args {
		if arg == "-api" {
			Flags = flags()
		}
	}
}

func flags() *FlagConfig {
	fc := new(FlagConfig)
	//hostname, _ := os.Hostname()
	flag.IntVar(&(fc.MaxConnections), "tw", MAX_CONNECTIONS, "API : Maximum number of streamers. (-1 for inf.)")
	flag.BoolVar(&(fc.Api), "api", false, "API : Enable api proces. ")
	flag.StringVar(&(fc.BindIp), "bindip", "127.0.0.1", "API : Address to bind to.")
	flag.StringVar(&(fc.EtcdEndpoints), "etcd", "http://127.0.0.1:2379", "API : Etcd addresses (comma-delimited)")
	flag.StringVar(&(fc.CassEndpoints), "cassandra", "127.0.0.1", "API : Cassandra addresses (comma-delimited)")
	flag.StringVar(&(fc.CassKeyspace), "keyspace", "chan", "API : Cassandra keyspace")
	flag.StringVar(&(fc.ClusterName), "clustername", "streamingchan", "API : Cluster name")
	flag.IntVar(&(fc.HttpPort), "httpport", PORT_NUMBER, "Node : Host for HTTP Server for serving stats. 0 for disabled.")

	return fc
}
