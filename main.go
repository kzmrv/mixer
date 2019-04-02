package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
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
	results := make([]chan *callResult, len(works))
	var wg sync.WaitGroup
	wg.Add(len(works))
	for i, work := range works {
		results[i] = make(chan *callResult, 100000) // todo parallel receivers
		go dispatch(&wg, work, workers, results[i])
	}

	wg.Wait()

	for i := 0; i < len(works); i++ {
		counter := -1
		for hasMore := true; hasMore; {
			_, hasMore = <-results[i]
			counter++
		}
		log.Infof("File %v found %d matching lines", works[i].File, counter)
	}

	log.Info("App finished")
}

func getNextRequest() *userRequest {
	return &userRequest{
		buildNumber:     300,
		filePrefix:      "kube-apiserver-audit.log-",
		targetSubstring: "2019-01-25T16:21:19.61",
	}
}

var i = -1

// Round robin dispatch
func dispatch(wg *sync.WaitGroup, work *pb.Work, workers []pb.WorkerClient, ch chan *callResult) {
	defer close(ch)
	defer wg.Done()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*timeoutSeconds)
	defer cancel()
	i++
	client, err := workers[i%len(workers)].DoWork(ctx, work)
	if err != nil {
		ch <- &callResult{err: err}
		return
	}
	for {
		workRes, err := client.Recv()
		if err == io.EOF {
			return
		}
		res := &callResult{workResult: workRes, err: err}
		ch <- res
		if err != nil {
			return
		}
	}
}

type callResult struct {
	workResult *pb.WorkResult
	err        error
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
