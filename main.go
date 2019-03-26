package main

import (
	"context"
	"time"

	pb "github.com/kzmrv/mixer/proto"
	"google.golang.org/grpc"
	log "k8s.io/klog"
)

const (
	address        = "localhost:17654"
	timeoutSeconds = 240
)

func main() {
	log.InitFlags(nil)

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	cl := pb.NewWorkerClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*timeoutSeconds)
	defer cancel()
	res, err := cl.DoWork(ctx, &pb.Work{File: "logs/ci-kubernetes-e2e-gce-scale-performance/290/artifacts/gce-scale-cluster-master/kube-apiserver-audit.log-20190102-1546444805.gz", TargetSubstring: "\"auditID\":\"07ff64df-fcfe-4cdc-83a5-0c6a09237698\""})
	if err != nil {
		log.Fatal(err)
	}
	log.Info(res.Logs[0].Entry)
}
