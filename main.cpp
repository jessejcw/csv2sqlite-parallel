//
// Created by jessew on 2/11/18.
//

#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <memory.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>



#define ERROR_NONE  0
#define ERROR_SOME -1

using namespace std;

typedef int (*CB_FUNC) (vector<string> data, int fd, void* worker_mgr);
typedef std::tuple<CB_FUNC, vector<string>, int, void*> JobPkg;

class Producer;
class Worker {

    // based on cpu core
    unsigned thread_count_;

    // invoke thread
    std::mutex thread_create_mutex_;
    std::recursive_mutex job_queue_mutex_;
    std::recursive_mutex syncing_mutex_;

    unsigned invoked_worker_ = 0;
    bool worker_invoked_cond_var_ = false;
    bool producer_wait_ = false;
    bool is_syncing_wait_ = false;

    condition_variable thread_create_cv;
    condition_variable_any consumer_cv;
    condition_variable_any producer_cv;
    condition_variable_any syncing_cv;
    vector<std::thread> worker_threads;

    // critical section
    queue<JobPkg> job_queue;
    int idle_workr =0;
    unsigned comsumer_sync_cnt =0;
    static const int Killer = -777;
    vector<string>  headers_;

    // main worker function
    static void func_worker_main(void *data, int id) {
        Worker* mgr = static_cast<Worker*>(data);

        // spawn the consumer threads and wait
        mgr->invoke_workr();

        std::cout<< "thread:" <<id <<std::endl;

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
                //mgr->wakeup_producer();
            }
            else {
                mgr->set_exit();
                break;
            }
        }
    }

    void idle() {
        idle_workr++;
    }
    void wakeup_producer() {
        //unique_lock<mutex> lock1(job_mutex);
        if (producer_wait_ == true) {
            // potential data race! dont care
            if (job_queue.size() < 99 ) {
                producer_cv.notify_one();
            }
        }
    }

    void wakeup_consumer() {
        unique_lock<recursive_mutex> lock1(job_queue_mutex_);
        if (idle_workr) {
            consumer_cv.notify_one();
        }
    }
    void set_exit() {
        unique_lock<recursive_mutex> lock1(syncing_mutex_);
        ++comsumer_sync_cnt;
        if (is_syncing_wait_) {
            syncing_cv.notify_one();
        }
    }

    // callback
    void process_job(JobPkg& job) {
        std::get<0>(job)(std::get<1>(job), std::get<2>(job), std::get<3>(job));
    }

    void dequeue_job(JobPkg &ret_pkg) {

        std::unique_lock<recursive_mutex> lk(job_queue_mutex_);

        //signal producer if any
        if (producer_wait_) {
            producer_cv.notify_one();
        }

        // unfinished job
        if (is_syncing_wait_) {
            syncing_cv.notify_one();
        }

        while (job_queue.empty()) {
            ++idle_workr;
            consumer_cv.wait(lk);
            --idle_workr;
        }

        ret_pkg = job_queue.front();

        if (get<2>(ret_pkg) != Killer)
            job_queue.pop();
    }

    void invoke_workr() {
        std::unique_lock<mutex> lk(thread_create_mutex_);
        ++invoked_worker_;
        if (worker_invoked_cond_var_ && invoked_worker_ == thread_count_)
            thread_create_cv.notify_one();
    }

public:

    // call from the main thread
    // the queue will dispatch the job automatically
    void enqueue_job(JobPkg &job) {
        std::unique_lock<recursive_mutex> lock(job_queue_mutex_);
        while(job_queue.size() > 99 ) {
            // job queue has over 99 job haven't been process
            // blocking producer
            producer_wait_ = true;
            producer_cv.wait(lock);

            if (idle_workr != 0) {
                consumer_cv.notify_all();
            }

            producer_wait_ = false;
        }
        job_queue.push(job);

        if (idle_workr != 0) {
            consumer_cv.notify_all();
        }
    }

    ~Worker() {}
    int create_workers() {

        // num of threads that cpu support minus main thread
        thread_count_ = std::thread::hardware_concurrency()-1;
        worker_threads.resize(thread_count_);
        for(unsigned i =0; i < thread_count_; ++i) {
            worker_threads[i] = std::thread(Worker::func_worker_main, (void *) this, i);
        }

        while (invoked_worker_ != thread_count_) {
            std::unique_lock<mutex> lk(thread_create_mutex_);
            worker_invoked_cond_var_ = true;
            thread_create_cv.wait(lk);
            worker_invoked_cond_var_ = false;
        }
        return ERROR_NONE;
    }


    void flush() {

        JobPkg killer_job({nullptr, {}, -777, (void*)this});
        enqueue_job(killer_job);

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



    }
    void join() {
        for (unsigned i =0; i < thread_count_; i++) {
            cout<<"join:"<<i<<endl;
            worker_threads[i].join();
        }
    }

    vector<string>& get_headers() {
        return headers_;
    }
};

#define ERROR_NONE  0
#define ERROR_SOME -1

static int cnt = 0;
static int create_record(vector<string>& header, vector<string>& row) {

    vector<char*> cargs((header.size()*2)+2);
    string cmd = "./csv_worker";
    cargs[0] = const_cast<char*>(cmd.c_str());
    cargs[1] = const_cast<char*>(to_string(header.size()).c_str());

    for (unsigned i = 2, j =0; i < cargs.size() && j <header.size(); i += 2, j++) {
        cargs[i] = const_cast<char*>(header[j].c_str());
        cargs[i+1] = const_cast<char*>(row[j].c_str());
    }
    cargs.push_back((char*)'\0');

    pid_t intermediate_pid = fork();
    if (intermediate_pid == 0) {
        pid_t worker_pid = fork();
        if (worker_pid == 0) {

            execvp(cmd.c_str(), &cargs[0]);

            _exit(0);
        }

        pid_t timeout_pid = fork();
        if (timeout_pid == 0) {
            sleep(100);
            _exit(0);
        }

        pid_t exited_pid = wait(NULL);
        if (exited_pid == worker_pid) {
            kill(timeout_pid, SIGKILL);
        } else {
            cout<<"[Kill]"<<timeout_pid<<endl;
            kill(worker_pid, SIGKILL); // Or something less violent if you prefer
        }
        wait(NULL); // Collect the other process
        _exit(0); // Or some more informative status
    }
    waitpid(intermediate_pid, 0, 0);

    return ERROR_NONE;
}

// [DB] main worker thread func
int process_job(vector<string> row, int ctl, void* data) {

    if (++cnt % 100 == 0)
        std::cout<< "processing..." << cnt<<std::endl;

    Worker* mgr = static_cast<Worker*>(data);
    vector<string>& headers = mgr->get_headers();

    int len_row = row.size(), len_header = headers.size();

    if (!len_row || !len_header ) {
        cout<<"[drop]:"<<cnt<<" row_len:"<<len_row<<" header_len:"<<len_header<<endl;
        return 1;
    }
    create_record(headers, row);
    return 0;
}

class FileHdlr {
public:
    enum class USE_TYPE { UNKNOWN, USE, SKIP};

    vector<string> read_file(fstream &fs) {
        vector<string> res;
        string out;
        while (getline(fs, out )) {
            res.push_back(out);
        }
        return res;
    }

    vector<string> read_line(string& line) {
        vector<string> res;
        string out;
        istringstream ss(line);
        while(getline(ss, out, ',')) {
            res.push_back(out);
        }

        return res;
    }
    vector<string> parse_header(string& line, vector<int>& cols_skip) {
        const int sz = line.size();
        if (!sz) {return {};}

        // handle unname
        if (line[0] == ',') {
            line = "id" + line;
        }

        istringstream ss(line);
        string out;

        vector<string> headers;
        while (getline(ss, out, ',' )) {
            headers.push_back(out);
        }
        use_column_ = vector<USE_TYPE>(headers.size(), USE_TYPE::USE);
        for (int idx : cols_skip) {
            if (idx >= sz) {
                cout<<"\nError:\ncolumns size:"<<sz << " skip column:<<idx"<<endl;
                return {};
            }
            use_column_[idx] = USE_TYPE::SKIP;
        }

        return headers;
    }


private:
    vector<USE_TYPE> use_column_;
};

class Producer {
    queue<int> pid;
    Worker worker_;
public:
    Producer() {
        if (ERROR_NONE != worker_.create_workers())
            return;
    }

    void process_row(vector<string>& header, vector<string>& row) {


        vector<string>& headers = worker_.get_headers();
        if (headers.size() < header.size()) {
            headers = std::move(header);
        }

        //packaging job
        JobPkg job = {process_job, row, 0, (void*)&worker_};
        worker_.enqueue_job(job);
    }

    int shutdown() {
        worker_.flush();
        worker_.join();
        return 1;
    }
};

int main(int argc, char* argv[]) {

    if (argc != 2) {
        cout<<"usage: ./csv_to_cll <fname>.csv"<<endl;
        exit(0);
    }

    string fname = argv[1];
    fstream theFile(fname.c_str());
    if (!theFile) {
        cout<<"Error: open file "<<fname<<endl;
        exit(0);
    }

    FileHdlr fh;

    vector<string> lines = fh.read_file(theFile);
    vector<string> headers;
    vector<int> skip_col;

    Producer pdr;

    for (unsigned i =0; i < lines.size(); i++) {
        if (i == 0) {
            headers = fh.parse_header(lines[i], skip_col);
        } else {
            vector<string> one_row = fh.read_line(lines[i]);
            // process row
            pdr.process_row(headers, one_row);
        }
    }

    pdr.shutdown();
    cout<<"[Total Records]:"<<cnt<<endl;

    return 1;
}
