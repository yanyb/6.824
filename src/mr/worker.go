package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"log"
	"net/rpc"
	"hash/fnv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KeyValueSlice []KeyValue

func (s KeyValueSlice) Len() int           { return len(s) }
func (s KeyValueSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s KeyValueSlice) Less(i, j int) bool { return s[i].Key < s[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := AskForTaskArgs{}
		reply := AskForTaskReply{}
		call("Coordinator.AskForTask", &args, &reply)
		if reply.Done {
			return
		}
		if reply.Task == nil {
			continue
		}
		if reply.Task.Type == TaskType_Map {
			intermediate := []KeyValue{}
			nReduce := reply.ReduceN
			for _, filename := range reply.Task.InputFile {
				content, err := ioutil.ReadFile(filename)
				if err != nil {
					log.Fatal(err)
				}
				kva := mapf(filename, string(content))
				intermediate = append(intermediate, kva...)
			}
			intermediateFile := map[int]string{}
			fileMap := map[int]*os.File{}
			sort.Sort(KeyValueSlice(intermediate))
			for _, kv := range intermediate {
				i := ihash(kv.Key) % nReduce
				if _, has := intermediateFile[i+1]; !has {
					f, err := ioutil.TempFile(".", "mr-")
					if err != nil {
						log.Fatal(err)
					}
					intermediateFile[i+1] = f.Name()
					fileMap[i+1] = f
				}
				f := fileMap[i+1]
				enc := json.NewEncoder(f)
				if err := enc.Encode(&kv); err != nil {
					log.Println(err)
					continue
				}
			}

			ofiles := []string{}
			for i, filename := range intermediateFile {
				ofile := fmt.Sprintf("mr-%d-%d", reply.Task.Index, i)
				ofiles = append(ofiles, ofile)
				os.Rename(filename, ofile)
			}

			for _, f := range fileMap {
				f.Close()
			}
			call("Coordinator.FinishTask", &FinishTaskArgs{Type: reply.Task.Type,
				Index: reply.Task.Index, OutputFile: ofiles}, &FinishTaskReply{})
		} else {
			intermediate := []KeyValue{}
			for _, filename := range reply.Task.InputFile {
				f, err := os.Open(filename)
				if err != nil {
					log.Fatal(err)
				}
				dec := json.NewDecoder(f)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(KeyValueSlice(intermediate))

			f, err := ioutil.TempFile(".", "mr-")
			if err != nil {
				log.Fatal(err)
			}
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				fmt.Fprintf(f, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			os.Rename(f.Name(), fmt.Sprintf("mr-out-%d", reply.Task.Index))
			f.Close()
			call("Coordinator.FinishTask", &FinishTaskArgs{Type: reply.Task.Type,
				Index: reply.Task.Index}, &FinishTaskReply{})
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
