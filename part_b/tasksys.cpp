#include "tasksys.h"

#include <algorithm>
#include <chrono>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    num_threads_ = std::max(1, num_threads);
    run_runnable_ = nullptr;
    run_total_tasks_ = 0;
    run_next_task_id_.store(0);
    run_completed_tasks_.store(0);
    run_has_work_ = false;
    unfinished_launches_ = 0;
    shutdown_ = false;

    for (int i = 0; i < num_threads_; i++) {
        workers_.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    sync();

    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_ = true;
    }
    work_cv_.notify_all();

    for (std::thread& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    if (num_total_tasks <= 0) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        run_runnable_ = runnable;
        run_total_tasks_ = num_total_tasks;
        run_next_task_id_.store(0);
        run_completed_tasks_.store(0);
        run_has_work_ = true;
    }
    work_cv_.notify_all();

    while (executeRunChunk()) {
    }

    std::unique_lock<std::mutex> lock(mutex_);
    run_done_cv_.wait(lock, [this]() {
        return run_completed_tasks_.load() == run_total_tasks_;
    });
}

void TaskSystemParallelThreadPoolSleeping::enqueueReadyLaunchLocked(TaskID id) {
    LaunchState& launch = launches_[id];
    if (launch.completed) {
        return;
    }

    if (launch.total_tasks <= 0) {
        completeLaunchLocked(id);
        return;
    }

    ready_launches_.push_back(id);
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {
    std::unique_lock<std::mutex> lock(mutex_);

    TaskID id = static_cast<TaskID>(launches_.size());
    LaunchState launch;
    launch.runnable = runnable;
    launch.total_tasks = num_total_tasks;
    launch.next_task_id = 0;
    launch.completed_tasks = 0;
    launch.remaining_dependencies = 0;
    launch.task_chunk_size = 1;
    launch.chunk_size_chosen = num_total_tasks <= num_threads_;
    launch.completed = false;
    launches_.push_back(launch);

    for (TaskID dep : deps) {
        if (dep >= 0 && dep < id && !launches_[dep].completed) {
            launches_[dep].dependents.push_back(id);
            launches_[id].remaining_dependencies++;
        }
    }

    unfinished_launches_++;

    if (launches_[id].remaining_dependencies == 0) {
        enqueueReadyLaunchLocked(id);
        work_cv_.notify_all();
        if (unfinished_launches_ == 0) {
            sync_cv_.notify_all();
        }
    }

    return id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    while (true) {
        if (executeReadyChunk()) {
            continue;
        }

        std::unique_lock<std::mutex> lock(mutex_);
        if (unfinished_launches_ == 0) {
            return;
        }
        sync_cv_.wait(lock, [this]() {
            return unfinished_launches_ == 0 || !ready_launches_.empty();
        });
    }
}

void TaskSystemParallelThreadPoolSleeping::completeLaunchLocked(TaskID id) {
    LaunchState& launch = launches_[id];
    if (launch.completed) {
        return;
    }

    launch.completed = true;
    unfinished_launches_--;

    std::vector<TaskID> dependents = launch.dependents;
    for (TaskID dependent_id : dependents) {
        LaunchState& dependent = launches_[dependent_id];
        dependent.remaining_dependencies--;
        if (dependent.remaining_dependencies == 0) {
            enqueueReadyLaunchLocked(dependent_id);
        }
    }

    sync_cv_.notify_all();
}

bool TaskSystemParallelThreadPoolSleeping::executeRunChunk() {
    IRunnable* runnable = nullptr;
    int total_tasks = 0;
    int start_task_id = 0;
    int end_task_id = 0;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!run_has_work_ || run_next_task_id_.load() >= run_total_tasks_) {
            return false;
        }
        runnable = run_runnable_;
        total_tasks = run_total_tasks_;
        int task_chunk_size = (total_tasks < num_threads_) ? 1 : 2;
        start_task_id = run_next_task_id_.load();
        end_task_id = std::min(start_task_id + task_chunk_size, total_tasks);
        run_next_task_id_.store(end_task_id);
    }

    for (int task_id = start_task_id; task_id < end_task_id; task_id++) {
        runnable->runTask(task_id, total_tasks);
    }

    int completed_now = end_task_id - start_task_id;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        int new_completed = run_completed_tasks_.load() + completed_now;
        run_completed_tasks_.store(new_completed);
        if (new_completed == total_tasks) {
            run_has_work_ = false;
            run_done_cv_.notify_one();
        }
    }

    return true;
}

bool TaskSystemParallelThreadPoolSleeping::executeReadyChunk() {
    TaskID launch_id = 0;
    IRunnable* runnable = nullptr;
    int total_tasks = 0;
    int start_task_id = 0;
    int end_task_id = 0;
    bool choose_chunk_size = false;
    bool requeued_work = false;

    {
        std::unique_lock<std::mutex> lock(mutex_);
        while (!ready_launches_.empty()) {
            launch_id = ready_launches_.front();
            ready_launches_.pop_front();

            LaunchState& launch = launches_[launch_id];
            if (!launch.completed && launch.next_task_id < launch.total_tasks) {
                int task_chunk_size = launch.task_chunk_size;
                choose_chunk_size = !launch.chunk_size_chosen;
                start_task_id = launch.next_task_id;
                end_task_id = std::min(start_task_id + task_chunk_size, launch.total_tasks);
                launch.next_task_id = end_task_id;

                if (launch.next_task_id < launch.total_tasks) {
                    ready_launches_.push_back(launch_id);
                    requeued_work = true;
                }

                runnable = launch.runnable;
                total_tasks = launch.total_tasks;
                break;
            }
        }

        if (runnable == nullptr) {
            return false;
        }
    }

    if (requeued_work) {
        work_cv_.notify_one();
        sync_cv_.notify_one();
    }

    std::chrono::high_resolution_clock::time_point chunk_start_time;
    if (choose_chunk_size) {
        chunk_start_time = std::chrono::high_resolution_clock::now();
    }

    for (int task_id = start_task_id; task_id < end_task_id; task_id++) {
        runnable->runTask(task_id, total_tasks);
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        LaunchState& launch = launches_[launch_id];
        if (choose_chunk_size && !launch.chunk_size_chosen) {
            auto chunk_end_time = std::chrono::high_resolution_clock::now();
            long long elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                chunk_end_time - chunk_start_time).count();
            if (elapsed_ns < 2000) {
                launch.task_chunk_size = 8;
            } else if (elapsed_ns < 20000) {
                launch.task_chunk_size = 4;
            } else {
                launch.task_chunk_size = 2;
            }
            launch.chunk_size_chosen = true;
        }
        launch.completed_tasks += end_task_id - start_task_id;
        if (launch.completed_tasks == launch.total_tasks) {
            completeLaunchLocked(launch_id);
            work_cv_.notify_all();
        }
    }

    return true;
}

void TaskSystemParallelThreadPoolSleeping::workerLoop() {
    while (true) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            work_cv_.wait(lock, [this]() {
                return shutdown_ || !ready_launches_.empty() ||
                       (run_has_work_ && run_next_task_id_.load() < run_total_tasks_);
            });

            if (shutdown_) {
                return;
            }

            if (run_has_work_ && run_next_task_id_.load() < run_total_tasks_) {
                lock.unlock();
                executeRunChunk();
                continue;
            }
        }

        executeReadyChunk();
    }
}
