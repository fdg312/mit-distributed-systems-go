package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string

func Worker(sockname string, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	coordSockName = sockname
	for {
		args := AskTaskArgs{}
		reply := AskTaskReply{}

		ok := call("Coordinator.AskTask", &args, &reply)
		if !ok {
			break
		}

		switch reply.TaskType {
		case TaskTypeMap:
			doMapTask(reply, mapf)
		case TaskTypeReduce:
			doReduceTask(reply, reducef)
		case TaskTypeWait:
			time.Sleep(time.Second)
		case TaskTypeExit:
			return
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		return false
	}

	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

func doMapTask(reply AskTaskReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()

	kva := mapf(reply.Filename, string(content))

	files := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)

	for i := 0; i < reply.NReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)

		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("cannot create file: %v", err)
		}
		files[i] = file

		encode := json.NewEncoder(file)
		encoders[i] = encode
	}

	for _, item := range kva {
		bucket := ihash(item.Key) % reply.NReduce
		encoders[bucket].Encode(&item)
	}

	for _, file := range files {
		file.Close()
	}

	args := ReportTaskArgs{}

	args.TaskID = reply.TaskID
	args.TaskType = reply.TaskType

	reportReply := ReportTaskReply{}

	call("Coordinator.ReportTask", &args, &reportReply)
}

func doReduceTask(reply AskTaskReply, reducef func(string, []string) string) {
	kva := []KeyValue{}

	for i := 0; i < reply.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reply.TaskID)
		file, err := os.Open(filename)
		if err != nil {
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	tmpFile, err := os.CreateTemp("", "mr-tmp-*")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		output := reducef(kva[i].Key, values)

		fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	tmpFile.Close()

	outFilename := fmt.Sprintf("mr-out-%d", reply.TaskID)
	os.Rename(tmpFile.Name(), outFilename)

	args := ReportTaskArgs{
		TaskID:   reply.TaskID,
		TaskType: TaskTypeReduce,
	}
	reportReply := ReportTaskReply{}
	call("Coordinator.ReportTask", &args, &reportReply)
}
