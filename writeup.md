# Assignment 2 Writeup

## 1. Task System Implementation

### Overview

This assignment implements a task execution library for bulk task launches. A
bulk launch calls:

```cpp
runnable->runTask(task_id, num_total_tasks);
```

for every task id in the range `[0, num_total_tasks)`. For Part A, `run()` must
be synchronous: when `run()` returns, every task in that bulk launch has already
completed.

Our Part A implementation uses dynamic task assignment for all parallel systems.
Instead of giving each worker a fixed range of task ids before execution starts,
workers claim task ids from shared state while the bulk launch is running. This
was useful because the runtime does not know whether each task has equal cost.
For example, `ping_pong_unequal` has uneven task costs, so dynamic assignment
allows workers that finish early to take more work.

### `TaskSystemParallelSpawn`

`TaskSystemParallelSpawn::run()` creates worker threads on every bulk launch.
The implementation keeps a shared `next_task_id` counter protected by a mutex.
Each worker repeatedly:

1. locks the mutex,
2. claims the next available task id,
3. unlocks the mutex,
4. runs `runnable->runTask(task_id, num_total_tasks)`.

When all task ids have been claimed, the worker exits. The main thread joins all
worker threads before `run()` returns. This makes the synchronous behavior easy
to reason about: returning from `run()` means every spawned worker has finished.

This implementation is simple and correct, but it pays thread creation and join
overhead on every call to `run()`.

### `TaskSystemParallelThreadPoolSpinning`

`TaskSystemParallelThreadPoolSpinning` creates a persistent worker pool in the
constructor. The workers stay alive across multiple calls to `run()`, so the
runtime avoids repeated thread creation.

Each call to `run()` publishes a new batch by setting shared state:

- `current_runnable_`
- `total_tasks_`
- `next_task_id_`
- `completed_tasks_`
- `has_work_`

Workers loop forever until destruction. When work is available, a worker claims
one task id from `next_task_id_`, runs it, and increments `completed_tasks_`.
When no work is available, workers spin and call `std::this_thread::yield()`.
The main thread also spins until `completed_tasks_ == total_tasks_`, then clears
`has_work_` and returns.

The destructor sets `shutdown_ = true` and joins all persistent workers. This is
where the worker threads are actually terminated. Individual calls to `run()` do
not join them.

### `TaskSystemParallelThreadPoolSleeping`

`TaskSystemParallelThreadPoolSleeping` also uses a persistent worker pool, but
workers sleep when there is no work instead of spinning. It uses two condition
variables:

- `work_cv_` wakes workers when a new bulk launch is available.
- `done_cv_` wakes the main thread when all tasks in the current bulk launch
  have completed.

The batch-level state is protected by a mutex:

- `current_runnable_`
- `total_tasks_`
- `has_work_`
- `shutdown_`

The hot task counters are atomic:

```cpp
std::atomic<int> next_task_id_;
std::atomic<int> completed_tasks_;
```

This avoids taking the mutex for every task claim and every completion update,
which helped on very lightweight workloads.

When `run()` starts, it resets the batch state, stores the runnable pointer and
task count, sets `has_work_ = true`, and calls `work_cv_.notify_all()`. The main
thread then participates in executing tasks instead of only waiting. This matters
for small tasks because the caller thread can do useful work while worker
threads are waking up.

Both the main thread and worker threads claim small chunks of task ids using
`next_task_id_.fetch_add(...)`. The final Part A implementation uses adaptive
chunking:

```text
if num_total_tasks < num_threads:
    chunk size = 1
else:
    chunk size = 2
```

Chunk size 1 preserves load balance when there are fewer tasks than workers.
Chunk size 2 reduces task-claim overhead for very small tasks while still
keeping enough dynamic scheduling flexibility for uneven workloads.

The sleeping implementation also had to avoid stale batch state. Workers read
the current runnable pointer and total task count under the mutex when claiming
a chunk, then run only that chunk before checking the shared state again. This
prevents a worker from accidentally continuing to use an old batch after a later
call to `run()` has started.

When the final task in a bulk launch completes, the completing thread clears
`has_work_` and notifies `done_cv_`. The caller thread waits on `done_cv_` until
`completed_tasks_ == total_tasks_`, which preserves the required synchronous
behavior of `run()`.

### Part B Dependency Tracking

For Part B, we implemented dependency tracking only in
`TaskSystemParallelThreadPoolSleeping`, as required by the assignment.

Each call to `runAsyncWithDeps()` creates one launch record and returns its
`TaskID` immediately. A launch record stores:

- the runnable pointer,
- the total number of tasks in the launch,
- the next task id to assign,
- the number of completed tasks,
- the number of dependencies that are still incomplete,
- the list of dependent child launches,
- whether the launch has completed.

If a new launch has no incomplete dependencies, it is placed in the ready queue.
If it still depends on earlier launches, it remains waiting. For each incomplete
dependency, the new launch id is appended to the dependency's child list.

Worker threads sleep on a condition variable until the ready queue contains
work. A ready queue entry is a launch id. When a worker or the thread calling
`sync()` takes a ready launch, it claims a chunk of task ids from that launch,
runs those tasks, and updates the launch's completed-task count. If the launch
still has remaining unclaimed tasks, it is placed back into the ready queue so
other workers can help.

When a launch's completed-task count reaches its total task count, the launch is
marked complete. The runtime then visits all of its dependent child launches and
decrements their remaining dependency counts. Any child whose count reaches zero
is moved to the ready queue.

The runtime also maintains `unfinished_launches_`, the number of submitted async
launches that have not completed. `runAsyncWithDeps()` increments this count,
and launch completion decrements it. `sync()` waits until this count becomes
zero. While waiting, `sync()` also helps execute ready chunks instead of only
sleeping.

For Part B's synchronous `run()`, we kept a separate fast path modeled after the
Part A sleeping thread pool. This avoids sending synchronous Part A-style
workloads through the heavier dependency-graph queue. The assignment states that
programs will either use `run()` or `runAsyncWithDeps()`, so these two paths do
not need to support being mixed by the same application.

One important performance detail is adaptive chunking for async launches. A
launch starts with a one-task sample. If that sample is extremely small, later
chunks are larger to reduce queue overhead. If the sample is more expensive,
later chunks stay smaller to preserve load balance. This helped both tiny
workloads such as `super_super_light_async` and uneven workloads such as
`ping_pong_unequal_async`.

## 2. Part A Performance Discussion

The final Part A harness was run on the AWS ARM instance used for the
assignment. The archived result is:

```text
artifacts/part_a/sleeping/final_full_harness.txt
```

All Part A implementations passed the provided performance checks:

```text
[Serial]                                : All passed Perf
[Parallel + Always Spawn]               : All passed Perf
[Parallel + Thread Pool + Spin]         : All passed Perf
[Parallel + Thread Pool + Sleep]        : All passed Perf
```

Some selected results from that run are shown below. Times are in milliseconds.

| Test | Serial | Spawn | Spin Pool | Sleep Pool |
| --- | ---: | ---: | ---: | ---: |
| `super_super_light` | 10.270 | 135.600 | 18.567 | 10.355 |
| `super_light` | 88.024 | 136.386 | 18.363 | 15.039 |
| `ping_pong_equal` | 1410.499 | 191.736 | 102.382 | 102.790 |
| `recursive_fibonacci` | 1071.247 | 75.521 | 68.195 | 68.185 |
| `math_operations_in_tight_for_loop` | 540.621 | 683.212 | 91.926 | 107.993 |
| `math_operations_in_tight_for_loop_fewer_tasks` | 540.434 | 357.828 | 97.866 | 110.832 |
| `spin_between_run_calls` | 382.243 | 191.405 | 191.596 | 191.363 |
| `mandelbrot_chunked` | 410.401 | 26.083 | 28.882 | 25.922 |

### Why Simple Implementations Can Perform Well

Parallelism has overhead. A task system must create or wake threads, coordinate
shared counters, and wait for completion. When each task does very little work,
this overhead can dominate the useful computation.

`super_super_light` is the clearest example. The serial implementation took
10.270 ms, while the sleeping thread pool took 10.355 ms. The serial system is
competitive because each task is tiny, so avoiding synchronization overhead is
almost as valuable as using multiple cores.

### When Sequential Execution Is Strong

Sequential execution performs best when the total amount of useful work per task
is too small to pay for scheduling overhead. In `super_super_light`, each task
is cheap enough that the serial implementation is essentially tied with the
sleeping thread pool. This does not mean serial execution is generally better;
it means the workload is too small for parallel scheduling to help much.

### When Spawn-Every-Launch Works Well

The spawn-every-launch implementation works well when each bulk launch contains
enough expensive work to hide thread creation overhead.

For example, `mandelbrot_chunked` took 410.401 ms serially and 26.083 ms with
spawn-per-launch. The task work is large enough that creating threads once for
the launch is not the bottleneck. In this case, the spawn implementation even
matched the sleeping thread pool closely: 26.083 ms for spawn versus 25.922 ms
for sleep.

`recursive_fibonacci` shows a similar pattern. It took 1071.247 ms serially and
75.521 ms with spawn-per-launch, because each task performs substantial
recursive computation.

### When Spawn-Every-Launch Is Poor

Spawn-per-launch performs poorly when there are many launches or the tasks are
very cheap. In `super_light`, spawn took 136.386 ms, while the spinning pool took
18.363 ms and the sleeping pool took 15.039 ms. The repeated cost of creating
and joining threads is much larger than the useful work in each task.

`math_operations_in_tight_for_loop` is another example where spawn was worse
than serial in the final run: serial took 540.621 ms, while spawn took
683.212 ms. The thread creation and scheduling overhead was not worth it for
that workload, but the persistent thread pools performed much better because
they reused workers.

### Why Thread Pools Help

Thread pools help by creating workers once and reusing them. This is especially
important when a test performs many small or medium-sized bulk launches. The
thread pool implementations avoid the repeated creation cost paid by
`TaskSystemParallelSpawn`.

The spinning pool is simple and fast because workers are always checking for new
work, but idle spinning consumes CPU resources. The sleeping pool uses condition
variables to avoid wasting CPU while idle. In the final implementation, the
sleeping pool also lets the main thread execute task chunks and uses atomic
counters, which made it competitive on both tiny and heavier workloads.

## 3. Custom Test

We implemented several synchronous custom tests:

- `zero_tasks_sync`
- `fewer_tasks_than_threads_sync`
- `repeated_launches_sync`
- `uneven_work_exact_once_sync`

The main test we would highlight is `repeated_launches_sync`.

### What The Test Does

`repeated_launches_sync` performs 40 consecutive calls to `run()`. Each launch
has 16 tasks. During launch `k`, each task writes the value `k` into its output
slot. Immediately after each `run()` returns, the test checks that every output
slot contains the current launch number.

### What The Test Checks

This test checks several important synchronous runtime properties:

- `run()` must not return before all tasks in the launch finish.
- Batch state must be reset correctly between launches.
- Workers must not keep using stale state from a previous launch.
- Every task id must run exactly once in every launch.

### How We Verified It

We ran the custom test on the AWS ARM instance using the assignment test
executable. The archived output is:

```text
artifacts/part_a/custom_tests/repeated_launches_sync.txt
```

The final full Part A harness was also archived at:

```text
artifacts/part_a/sleeping/final_full_harness.txt
```

For Part B, we added `async_returns_before_completion`. This test is specific to
`TaskSystemParallelThreadPoolSleeping`. It launches one slow async task and
checks that `runAsyncWithDeps()` returns before that task has completed, then
calls `sync()` and checks that the task did complete. This caught the original
starter behavior where `runAsyncWithDeps()` executed work synchronously before
returning.

The Part B custom and dependency-check output is archived at:

```text
artifacts/part_b/dependency_correctness_and_custom.txt
```

The final Part B harness output is archived at:

```text
artifacts/part_b/final_async_harness.txt
```

### Did This Test Change The Implementation?

Yes. While tuning the sleeping thread pool, repeated launches helped expose a
stale-batch race. An early version let worker threads keep local copies of the
runnable pointer and task count for too long. If one launch finished and another
started quickly, a worker could accidentally continue using old batch state.

The final implementation fixed this by making workers reread the current batch
state under the mutex whenever they claim a new chunk. This keeps each chunk
associated with the correct bulk launch.
