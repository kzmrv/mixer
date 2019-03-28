package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	pb "github.com/kzmrv/mixer/proto"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	log "k8s.io/klog"
)

const (
	address        = "localhost:17654"
	timeoutSeconds = 240
	bucketName     = "kubernetes-jenkins"
)

type userRequest struct {
	buildNumber     int
	filePrefix      string
	targetSubstring string
}

func main() {
	log.InitFlags(nil)
	connections, err := initWorkers()
	if err != nil {
		log.Fatalln(err)
	}

	workers := make([]pb.WorkerClient, len(connections))
	for i, connection := range connections {
		workers[i] = pb.NewWorkerClient(connection)
		defer connection.Close()
	}

	request := getNextRequest()
	works, err := getWorks(request)

	if err != nil {
		log.Fatalln(err)
	}

	for _, work := range works {
		// todo parallel async work
		res, err := dispatch(work, workers)
		if err != nil {
			log.Fatal(err)
		}
		println(res)
		break // todo remove
	}
}

func getNextRequest() *userRequest {
	return &userRequest{
		buildNumber:     300,
		filePrefix:      "kube-apiserver-audit.log-",
		targetSubstring: "asdasdad",
	}
}

var i = -1

// Round robin dispatch
func dispatch(work *pb.Work, workers []pb.WorkerClient) (*pb.WorkResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*timeoutSeconds)
	defer cancel()
	i++
	return workers[i%len(workers)].DoWork(ctx, work)
}

func initWorkers() ([]*grpc.ClientConn, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	workers := []*grpc.ClientConn{
		conn,
	}
	return workers, nil
}

func getWorks(request *userRequest) ([]*pb.Work, error) {
	prefix := fmt.Sprintf("logs/ci-kubernetes-e2e-gce-scale-performance/%v/artifacts/gce-scale-cluster-master/", request.buildNumber)
	files, err := getFiles(prefix, request.filePrefix)
	if err != nil {
		return nil, err
	}

	works := make([]*pb.Work, len(files))
	for i, file := range files {
		work := &pb.Work{File: file.Name, TargetSubstring: request.targetSubstring}
		works[i] = work
	}
	return works, nil
}

func getFiles(prefix string, substring string) ([]*storage.ObjectAttrs, error) {
	context := context.Background()
	client, err := storage.NewClient(context, option.WithoutAuthentication())
	if err != nil {
		return nil, err
	}

	bucket := client.Bucket(bucketName)
	allFiles := bucket.Objects(context, &storage.Query{Prefix: prefix})
	result := make([]*storage.ObjectAttrs, 0, allFiles.PageInfo().Remaining())
	var attr *storage.ObjectAttrs
	for {
		attr, err = allFiles.Next()
		if err != nil {
			break
		}
		if strings.Contains(attr.Name, substring) {
			result = append(result, attr)
		}
	}
	if err == iterator.Done {
		return result, nil
	}
	return nil, err
}
