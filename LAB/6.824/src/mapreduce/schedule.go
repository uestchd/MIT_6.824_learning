package mapreduce

import (
	"fmt"
	"sync"
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
	//
	// Initial mutex lock on workers
	workers := make([]string, 0)
	newWorker := false
	var mutex sync.Mutex
	cond_work := sync.NewCond(&mutex)
	go func() {
		for {
			wk := <- registerChan
			fmt.Println("New worker comes")
			newWorker = true
			mutex.Lock()
			workers = append(workers, wk)
			fmt.Printf("New worker registered: ", wk, "\n")
			newWorker = false
			cond_work.Broadcast()
			mutex.Unlock()
		}
	}()
	
	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		args := new(DoTaskArgs)
		args.JobName = jobName
		args.File = mapFiles[i]
		args.Phase = phase
		args.TaskNumber = i
		args.NumOtherPhase = n_other
		go func(taskid int){
			defer wg.Done()
			for {
				mutex.Lock()
				for len(workers) == 0 || newWorker{
					//fmt.Printf("worker number == 0, wait for connecting...\n")
					cond_work.Wait()
				}
				workerid := taskid % len(workers)
				ok := call(workers[workerid], "Worker.DoTask", args, new(struct{}))
				if ok == false {
					//fmt.Printf("DoTask: RPC %s call error, change to another worker\n", workers[workerid])
					taskid++
				} else {
					mutex.Unlock()
					break
				}
				mutex.Unlock()
			}
			//fmt.Printf("task %d done\n", taskid)
		}(i)	
	}
	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
