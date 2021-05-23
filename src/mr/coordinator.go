package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	Status_NonDistributed = 0
	Status_Distributed    = 1
	Status_Failed         = 2
	Status_Finished       = 3
)

const (
	TaskType_Map    = 1
	TaskType_Reduce = 2
)

type Coordinator struct {
	// Your definitions here.
	nReduce      int
	l            sync.Mutex
	mapTaskps    map[int]*Task
	reduceTasks  map[int]*Task
	intermediate []string
	reduceEnable bool
	done         bool
}

type Task struct {
	Type           int      // task type
	Index          int      // task index
	Status         int      // 0:未分配 1:已分配 2:失败 3:完成
	InputFile      []string // map or reduce input file name
	DistrubeteTime time.Time
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	c.l.Lock()
	if c.done {
		reply.Done = c.done
		c.l.Unlock()
		return nil
	}
	if !c.reduceEnable {
		for _, task := range c.mapTaskps {
			if task.Status == Status_Distributed && time.Now().Sub(task.DistrubeteTime) > 10*time.Second {
				task.Status = Status_Failed
			}
			if task.Status == Status_NonDistributed || task.Status == Status_Failed {
				task.Status = Status_Distributed
				task.DistrubeteTime = time.Now()
				reply.Task = task
				reply.ReduceN = c.nReduce
				return nil
			}
		}
	} else {
		for _, task := range c.reduceTasks {
			if task.Status == Status_Distributed && time.Now().Sub(task.DistrubeteTime) > 10*time.Second {
				task.Status = Status_Failed
			}
			if task.Status == Status_NonDistributed || task.Status == Status_Failed {
				task.Status = Status_Distributed
				task.DistrubeteTime = time.Now()
				reply.Task = task
				reply.ReduceN = c.nReduce
				return nil
			}
		}
	}
	c.l.Unlock()
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.l.Lock()
	defer c.l.Unlock()
	if args.Type == TaskType_Map {
		if t, has := c.mapTaskps[args.Index]; has && t.Status != Status_Finished {
			t.Status = Status_Finished
			c.intermediate = append(c.intermediate, args.OutputFile...)
		}
		done := true
		for _, task := range c.mapTaskps {
			if !(task.Status == Status_Finished) {
				done = false
				break
			}
		}
		c.reduceEnable = done

		if done {
			for _, imFile := range c.intermediate {
				var mapNum, reduceNum int
				if _, err := fmt.Sscanf(imFile, "mr-%d-%d", mapNum, reduceNum); err != nil {
					log.Println(err)
				} else {
					if _, has := c.reduceTasks[reduceNum]; !has {
						c.reduceTasks[reduceNum] = &Task{
							Type:      TaskType_Reduce,
							Index:     reduceNum,
							Status:    Status_NonDistributed,
							InputFile: []string{},
						}
					}
					c.reduceTasks[reduceNum].InputFile = append(c.reduceTasks[reduceNum].InputFile, imFile)
				}
			}
		}
	} else {
		if t, has := c.reduceTasks[args.Index]; has && t.Status != Status_Finished {
			t.Status = Status_Finished
		}

		done := true
		for _, task := range c.reduceTasks {
			if !(task.Status == Status_Finished) {
				done = false
				break
			}
		}
		c.done = done
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := c.done

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:      nReduce,
		mapTaskps:    map[int]*Task{},
		reduceTasks:  map[int]*Task{},
		intermediate: []string{},
	}

	// Your code here.
	for i, file := range files {
		c.mapTaskps[i+1] = &Task{
			Type:      TaskType_Map,
			Index:     i + 1,
			Status:    Status_NonDistributed,
			InputFile: []string{file},
		}
	}

	c.server()
	return &c
}
