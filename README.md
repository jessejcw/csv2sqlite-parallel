# csv2sqlite parallel

#### A simple parallel row processing with C++11 thread.
C++ concurent in one lesson

##Worker threads life cycle:

1. spawn.
2. sleep.
3. working.
4. synchronizing.
5. shutdown

#### 1. spawn
```cpp
   // num of threads that cpu support minus main thread
   thread_count_ = std::thread::hardware_concurrency()-1;
   worker_threads.resize(thread_count_);
   for(unsigned i =0; i < thread_count_; ++i) {
        worker_threads[i] = std::thread(Worker::func_worker_main, (void *) this, i);
   }

```

#### 2. sleep
```cpp   
    while (invoked_worker_ != thread_count_) {
        std::unique_lock<mutex> lk(thread_create_mutex_);
        worker_invoked_cond_var_ = true;
        thread_create_cv.wait(lk);
        worker_invoked_cond_var_ = false;
    }
```

#### 3. working
```cpp
    JobPkg curr_job;
    while (true) {
        // executed here when curr thread is done
        // with prev job and available for next job

        // blocking when job queue is empty
        mgr->dequeue_job(curr_job);

        int set = get<2>(curr_job);
        if (set != Killer) {
            // call back to requester
            mgr->process_job(curr_job);
        }
        else {
            mgr->set_exit();
            break;
        }
```

#### 4. synchronizing
used when you trying to purge the job queue and perform pass through
```cpp
    /* check if all consumers are flushed */
    syncing_mutex_.lock();
    while (comsumer_sync_cnt != thread_count_) {
        consumer_cv.notify_all();
        is_syncing_wait_ = true;
        syncing_cv.wait(syncing_mutex_);
    }
    comsumer_sync_cnt = 0;
    is_syncing_wait_ = false;
    syncing_mutex_.unlock();
```
#### 5. shutdown
steps to take:
1. send killer bit to job queue
2. purge job queue
3. join worker thread.
```cpp
    JobPkg killer_job({nullptr, {}, -777, (void*)this});
    enqueue_job(killer_job);
```