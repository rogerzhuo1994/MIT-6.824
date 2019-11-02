package mapreduce

import (
	"fmt"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	// Get all workers from registerChan, Loop until all tasks are finished
	// Firstly loop through all workers from registerChan, then wait for the done channel to have values
	// registerChan is also possible to get new workers when running, use select ... case
	// select ... case: either when a new worker is registered or done channel is written new values

	// Distribute a task: call DoTask RPC using call()
	// Distribute tasks in parallel: go + DoTask
	// Detect a task is finished: DoTask returns, use a channel to inform schedule()

	taskFinished := 0
	taskPool := makeSequence(ntasks)
	workerPool := []string{}

	worker := ""
	taskNumber := -1

	workerFinishChan := make(chan string)
	taskFailureChan := make(chan int)

	for {
		select {
		case taskNumber = <-taskFailureChan:
			fmt.Println("Schedule: task failed, ", taskNumber)
			taskPool = append(taskPool, taskNumber)
		default:
		}

		select {
		case worker = <-registerChan:
			fmt.Println("Schedule: newly registered worker found, ", worker)
			workerPool = append(workerPool, worker)
		case worker = <-workerFinishChan:
			fmt.Println("Schedule: available worker found, ", worker)
			workerPool = append(workerPool, worker)
			taskFinished += 1
		default:
		}

		// if all task is finished, end loop and return
		if taskFinished == ntasks {
			break
		}

		// both worker and task are available
		if len(taskPool) > 0 && len(workerPool) > 0{
			file := ""
			taskNumber = taskPool[0]
			taskPool = taskPool[1:]
			worker = workerPool[0]
			workerPool = workerPool[1:]

			switch phase {
			case mapPhase:
				file = mapFiles[taskNumber]
			case reducePhase:
				file = ""
			}
			go assignTask(jobName, worker, file, taskNumber, phase, n_other, workerFinishChan, taskFailureChan)
		}
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}

func assignTask(jobName string, workerName string, file string, taskNumber int, phase jobPhase, numOtherPhase int, workerFinishChan chan string, taskFailureChan chan int){
	args := DoTaskArgs{
		JobName: jobName,
		File: file,
		Phase: phase,
		TaskNumber: taskNumber,
		NumOtherPhase: numOtherPhase,
	}

	ok := call(workerName, "Worker.DoTask", &args, nil)
	if ok == false {
		fmt.Printf("Schedule: assignTask RPC failed...\n")
		taskFailureChan <- taskNumber
	}else {
		workerFinishChan <- workerName
	}
}

func makeSequence(n int) []int {
	arr := []int{}
	for i := 0; i < n; i++ {
		arr = append(arr, i)
	}
	return arr
}
