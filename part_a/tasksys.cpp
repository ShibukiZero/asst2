#include "tasksys.h"

#include <mutex>
#include <thread>
#include <vector>


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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
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
    num_threads_ = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    int next_task_id = 0;
    std::mutex task_mutex;
    std::vector<std::thread> workers;

    int worker_count = num_threads_;
    if (worker_count > num_total_tasks) {
        worker_count = num_total_tasks;
    }
    if (worker_count < 1) {
        worker_count = 1;
    }

    for (int i = 0; i < worker_count; i++) {
        workers.emplace_back([&]() {
            while (true) {
                int task_id;

                {
                    std::lock_guard<std::mutex> lock(task_mutex);
                    if (next_task_id >= num_total_tasks) {
                        return;
                    }
                    task_id = next_task_id;
                    next_task_id++;
                }

                runnable->runTask(task_id, num_total_tasks);
            }
        });
    }

    for (std::thread& worker : workers) {
        worker.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
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
    num_threads_ = num_threads;
    if (num_threads_ < 1) {
        num_threads_ = 1;
    }
    current_runnable_ = nullptr;
    total_tasks_ = 0;
    next_task_id_ = 0;
    completed_tasks_ = 0;
    has_work_ = false;
    shutdown_ = false;

    for (int i = 0; i < num_threads_; i++) {
        workers_.emplace_back(&TaskSystemParallelThreadPoolSpinning::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown_ = true;
    }

    for (std::thread& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    if (num_total_tasks <= 0) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        current_runnable_ = runnable;
        total_tasks_ = num_total_tasks;
        next_task_id_ = 0;
        completed_tasks_ = 0;
        has_work_ = true;
    }

    while (true) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (completed_tasks_ == total_tasks_) {
                has_work_ = false;
                return;
            }
        }

        std::this_thread::yield();
    }
}

void TaskSystemParallelThreadPoolSpinning::workerLoop() {
    while (true) {
        int task_id = -1;
        int total_tasks = 0;
        IRunnable* runnable = nullptr;

        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (shutdown_) {
                return;
            }

            if (has_work_ && next_task_id_ < total_tasks_) {
                task_id = next_task_id_;
                next_task_id_++;
                total_tasks = total_tasks_;
                runnable = current_runnable_;
            }
        }

        if (task_id >= 0) {
            runnable->runTask(task_id, total_tasks);

            {
                std::lock_guard<std::mutex> lock(mutex_);
                completed_tasks_++;
                if (completed_tasks_ == total_tasks_) {
                    has_work_ = false;
                }
            }
        } else {
            std::this_thread::yield();
        }
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
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
    num_threads_ = num_threads;
    if (num_threads_ < 1) {
        num_threads_ = 1;
    }
    current_runnable_ = nullptr;
    total_tasks_ = 0;
    next_task_id_.store(0);
    completed_tasks_.store(0);
    has_work_ = false;
    shutdown_ = false;

    for (int i = 0; i < num_threads_; i++) {
        workers_.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
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
        current_runnable_ = runnable;
        total_tasks_ = num_total_tasks;
        next_task_id_.store(0);
        completed_tasks_.store(0);
        has_work_ = true;
    }
    work_cv_.notify_all();

    int task_chunk_size = (num_total_tasks < num_threads_) ? 1 : 2;
    while (true) {
        int start_task_id = next_task_id_.fetch_add(task_chunk_size);
        if (start_task_id >= num_total_tasks) {
            break;
        }
        int end_task_id = start_task_id + task_chunk_size;
        if (end_task_id > num_total_tasks) {
            end_task_id = num_total_tasks;
        }

        for (int task_id = start_task_id; task_id < end_task_id; task_id++) {
            runnable->runTask(task_id, num_total_tasks);
        }

        int completed_now = end_task_id - start_task_id;
        if (completed_tasks_.fetch_add(completed_now) + completed_now == num_total_tasks) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                has_work_ = false;
            }
            done_cv_.notify_one();
        }
    }

    std::unique_lock<std::mutex> lock(mutex_);
    done_cv_.wait(lock, [this]() {
        return completed_tasks_.load() == total_tasks_;
    });
}

void TaskSystemParallelThreadPoolSleeping::workerLoop() {
    while (true) {
        int start_task_id = -1;
        int end_task_id = -1;
        int task_chunk_size = 1;
        int total_tasks = 0;
        IRunnable* runnable = nullptr;

        {
            std::unique_lock<std::mutex> lock(mutex_);
            work_cv_.wait(lock, [this]() {
                return shutdown_ || (has_work_ && next_task_id_.load() < total_tasks_);
            });

            if (shutdown_) {
                return;
            }

            task_chunk_size = (total_tasks_ < num_threads_) ? 1 : 2;
            start_task_id = next_task_id_.fetch_add(task_chunk_size);
            if (start_task_id >= total_tasks_) {
                continue;
            }
            end_task_id = start_task_id + task_chunk_size;
            if (end_task_id > total_tasks_) {
                end_task_id = total_tasks_;
            }
            total_tasks = total_tasks_;
            runnable = current_runnable_;
        }

        for (int task_id = start_task_id; task_id < end_task_id; task_id++) {
            runnable->runTask(task_id, total_tasks);
        }

        int completed_now = end_task_id - start_task_id;
        if (completed_tasks_.fetch_add(completed_now) + completed_now == total_tasks) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                has_work_ = false;
            }
            done_cv_.notify_one();
        }
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
