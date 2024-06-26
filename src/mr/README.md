[Lab instructions](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)

## Major points

- I did not fully read the `MapReduce` paper but only understood the basic workflow when doing this lab. Even for the parts that I did read, I did not always follow the paper.

- The coordinator does not maintain any worker state. 
In other words, the coordinator does not know how many workers are active, let alone which worker is doing/has done which task.

- Whenever the coordinator assigns a task to any worker, it creates a gorouting to monitor the task status. If the task is timeout, the coordinator will reassign the task.

## Implementation

### RPC

- There are 3 message types a worker can send to the coordinator: `Fetch`, `SubmitMap` and `SubmitReduce`.
  - `Fetch`: A worker requests a task with empty arguments. The coordinator replies with the `TaskType`, `TaskLocations` and `TaskNum`.
    - `TaskType`: The type can be one of `map`, `reduce`, `wait` or `done`.
    - `TaskLocations`: The location for a map task is a single input file path wrapped in a list. The location for a reduce task is a list of reduce folder paths, each corresponds to one input file, and stores `nReduce` reduce files. Each reduce file is named as a reduce task number beterrn 0 and `nReduce - 1`.
    - `TaskNum`: The number for a map task is always `nReduce`. The number for a reduce task is a reduce task number. 
  - `SubmitMap`: When a worker submits a map task, it submits the `TaskInput` and `SubmitLocation`. `TaskInput` stores the input file path, `SubmitLocation` stores the output reduce folder path. The coordinator replies with empty arguments.
  - `SubmitReduce`: When a worker submits a reduce task, it submits the `ReduceNum` and `SubmitLocation`. The `SubmitLocation` is the output file path with the prefix `mr-out-`.
  
### Worker

- A worker runs an infinite loop to fetch, do and submit a task until the coordinator replies a `done` in the `FetchReply`.
- When a worker does a map task, it first calls `mapf` function on the `TaskInput`, and splits the output to `nReduce` maps. The `mapf` output is a list of `KeyValue` objects, and the split is computed based on the key hash result. The worker then creates the reduce folder and `nReduce` files, then writes each split to its corresponding files.
- When a worker does a reduce task, it reads each file named as `reduceNum` in each reduce folder the coordinator sends. The worker iterates all `KeyValue`s in all files and aggregates all values of the same key in a list. The worker then calls `reducef(key, values)` for each unique key and value list, and appends the output to `mr-out-<reduceNum>`.

### Coordinator

- The coordinator maintains 4 task states, the read/write of any state requires a lock.
  - `openMapTasks`: a list of input file paths to be assigned to workers.
  - `doneMapTasks`: a map from the input file paths to the finished reduce folder path.
  - `openReduceTasks`: a list of reduce task numbers to be assigned to workers.
  - `doneReduceTasks`: a list of completed reduce task numbers.
- When initializing the coordinator, it also records `nFiles` and `nReduceTasks` to determine when all map tasks or reduce tasks are submitted.
- When a worker fetches a task, the coordinator first checks whether exists open map tasks; if not, it checks whether all map tasks are submitted; if not, it asks the worker to wait. If all map tasks are submitted, i.e., `len(doneMapTasks) == nFiles`, the coordinator assigns a reduce task if there exists any, otherwise it either sends `wait` or `done` depending on whether it has received all reduced tasks, i.e., `len(doneReduceTasks) == nReduceTasks`. The `openMapTasks` and `openReduceTasks` are updated accordingly.
- When a worker submits a map/reduce task, the coordinator updates `doneMapTasks`/`doneReduceTasks`. It also makes sure the task is removed from `openMapTasks`/`openReduceTasks`, as a task could have been timeout before it is submitted.
- Whenever the coordinator assigns a task, it starts a goroutine to monitor the task. The monitor gorouting sleeps until a timeout is reached, and checks whether the task has been submitted by checking `doneMapTasks` or `doneReduceTasks`. If not, it adds back the timeout task to `openMapTasks`/`openReduceTasks` to assign it again later.


## Clarafications

- The worker always creates `nReduce` reduce files even if some files are never written. This avoids the exception that a worker cannot find a file when doing a reduce task.
- The coordinator never creates any folder or file. It only stores a reduce folder location when a map task is complete. Therefore, it does not need to distinguish between a finished and unfinished reduce folder as it is never aware of any unfinished folder location.
- The same task may be submitted by multiple workers, e.g., the coordinator timeouts a task and reassigns it to another worker, and both workers submit it later. In this case, both workers write to the same file/folder, this does not affect the maximum length of `doneMapTasks` or `doneReduceTasks`.
- According to the paper, a worker needs to sort the keys in each reduce tasks, but my current `doReduceTask` implementation does not utilizes this property. Both sorted or unsorted solutions passed the tests in `test-mr.sh` with similar time consumption.

