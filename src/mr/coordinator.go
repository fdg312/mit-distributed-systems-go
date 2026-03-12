package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskIdle       = 0
	TaskInProgress = 1
	TaskCompleted  = 2
)

const (
	PhaseMap    = 0
	PhaseReduce = 1
	PhaseDone   = 2
)

type Task struct {
	ID       int
	Filename string
	Status   int
}

type Coordinator struct {
	mu sync.Mutex

	mapTasks    []Task
	reduceTasks []Task

	phase   int
	nReduce int

	mapDone    int
	reduceDone int
}

func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == PhaseDone {
		return true
	} else {
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nReduce = nReduce
	c.phase = PhaseMap

	mtasks := make([]Task, 0, len(files))

	for index, f := range files {
		task := Task{
			ID:       index,
			Filename: f,
			Status:   TaskIdle,
		}
		mtasks = append(mtasks, task)
	}

	c.mapTasks = mtasks

	rtasks := make([]Task, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		task := Task{
			ID:     i,
			Status: TaskIdle,
		}
		rtasks = append(rtasks, task)
	}
	c.reduceTasks = rtasks

	c.server(sockname)

	return &c
}

func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch c.phase {
	case PhaseMap:
		for i := range c.mapTasks {
			if c.mapTasks[i].Status == TaskIdle {
				c.mapTasks[i].Status = TaskInProgress

				reply.TaskType = TaskTypeMap
				reply.TaskID = c.mapTasks[i].ID
				reply.Filename = c.mapTasks[i].Filename
				reply.NReduce = c.nReduce
				reply.NMap = len(c.mapTasks)
				go func(taskID int) {
					time.Sleep(10 * time.Second)

					c.mu.Lock()
					defer c.mu.Unlock()

					if c.mapTasks[taskID].Status == TaskInProgress {
						c.mapTasks[taskID].Status = TaskIdle
					}

				}(c.mapTasks[i].ID)
				return nil
			}
		}

		reply.TaskType = TaskTypeWait
		return nil
	case PhaseReduce:
		for i := range c.reduceTasks {
			if c.reduceTasks[i].Status == TaskIdle {
				c.reduceTasks[i].Status = TaskInProgress
				reply.TaskType = TaskTypeReduce
				reply.TaskID = c.reduceTasks[i].ID
				reply.Filename = c.reduceTasks[i].Filename
				reply.NReduce = c.nReduce
				reply.NMap = len(c.mapTasks)
				go func(taskID int) {
					time.Sleep(10 * time.Second)

					c.mu.Lock()
					defer c.mu.Unlock()

					if c.reduceTasks[taskID].Status == TaskInProgress {
						c.reduceTasks[taskID].Status = TaskIdle
					}

				}(c.reduceTasks[i].ID)
				return nil
			}
		}

		reply.TaskType = TaskTypeWait
		return nil
	}

	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case TaskTypeMap:
		if c.mapTasks[args.TaskID].Status != TaskCompleted {
			c.mapTasks[args.TaskID].Status = TaskCompleted
			c.mapDone++
			if c.mapDone == len(c.mapTasks) {
				c.phase = PhaseReduce
			}
		}
		return nil
	case TaskTypeReduce:
		if c.reduceTasks[args.TaskID].Status != TaskCompleted {
			c.reduceTasks[args.TaskID].Status = TaskCompleted
			c.reduceDone++
			if c.reduceDone == len(c.reduceTasks) {
				c.phase = PhaseDone
			}
		}
		return nil
	}
	return nil
}
