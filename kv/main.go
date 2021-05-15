package main

import (
	"flag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"miniLinkDB/kv/config"
	"miniLinkDB/kv/server"
	"miniLinkDB/kv/storage"
	"miniLinkDB/kv/storage/raft_storage"
	"miniLinkDB/kv/storage/standalone_storage"
	"miniLinkDB/log"

	"miniLinkDB/proto/pkg/linkkvpb"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "net/http/pprof"

)

var (
	schedulerAddr = flag.String("scheduler", "", "scheduler address")
	storeAddr = flag.String("addr", "", "store address")
	dbPath = flag.String("path", "", "directory path of db")
	logLevel = flag.String("loglevel", "", "the level of log")
)

func main() {
	flag.Parse()
	conf := config.NewDefaultConfig()
	if *schedulerAddr != "" {
		conf.SchedulerAddr = *schedulerAddr
	}
	if *storeAddr != "" {
		conf.StoreAddr = *storeAddr
	}
	if *dbPath != "" {
		conf.DBPath = *dbPath
	}
	if *logLevel != "" {
		conf.LogLevel = *logLevel
	}

	log.SetLevelByString(conf.LogLevel)
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Infof("Server started with conf %+v", conf)

	var storage storage.Storage
	if conf.Raft {
		storage = raft_storage.NewRaftStorage(conf)
	} else {
		storage = standalone_storage.NewStandAloneStorage(conf)
	}

	if err := storage.Start(); err != nil {
		log.Fatal(err)
	}
	server := server.NewServer(storage)

	var alivePolicy = keepalive.EnforcementPolicy{
		MinTime: 2 * time.Second,
		PermitWithoutStream: true,
	}

	grpcServer := grpc.NewServer(
			grpc.KeepaliveEnforcementPolicy(alivePolicy),
			grpc.InitialWindowSize(1<<30),
			grpc.InitialConnWindowSize(1<<30),
			grpc.MaxRecvMsgSize(10 * 1024 * 1024),
			)
	linkkvpb.RegisterLinkKvServer(grpcServer, server)
	listenAddr := conf.StoreAddr[strings.IndexByte(conf.StoreAddr,':'):]
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	handleSignal(grpcServer)

	err = grpcServer.Serve(l)
	if err != nil {
		log.Fatal(err)
	}
	log.Info("Server stopped.")

}

func handleSignal(grpcServer *grpc.Server) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		sig := <- sigCh
		log.Infof("Got signal [%s] to exit.", sig)
		grpcServer.Stop()
	}()
}