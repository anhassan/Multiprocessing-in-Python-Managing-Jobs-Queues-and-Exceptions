# **Introduction**

The multiprocessing module in Python helps developers do many tasks at the same time, making things faster and more scalable. This is useful in different situations, like analyzing lots of data in big data projects or speeding up tasks in ETL workflows. It lets us run tasks in parallel, which means things get done quicker and more smoothly.

Now, let’s dive in and understand how to manage tasks, handle extra jobs, and deal with problems in multiprocessing setups. We’ll use examples to help explain things better and learn the best ways to use multiprocessing effectively.

# Queuing Extra Jobs

When the number of jobs surpasses the available processes, the extra jobs wait in the queue. For instance, if we have 3 jobs and 2 processes, and the execution times vary, the processes will handle the jobs concurrently, with the excess job waiting until a process becomes free. Let’s consider an example:

**Example:**  
Number of jobs = 3; Number of processes = 2  
Execution time of process 1 = 5 secs; Execution time of process 2 = 3 secs; Execution time of process 3 = 5 secs.  
**Result:**  
Process 1 & Process 2 start concurrently with Job1 and Job2 while Job3 waits in the queue  
Job2 finishes  
Job3 from the queue runs on Process 2  
Job1 and Job3 finish execution

![](https://miro.medium.com/v2/resize:fit:700/0*dJZMCeKMUZOgkXna)

# Handling Exceptions

In the event of a job failure, the next job in the queue continues execution. A single job failure doesn’t halt the execution of all pending jobs in the queue. Consider a scenario with 3 jobs and 2 processes. Even if one job fails, the other jobs proceed independently, ensuring efficient utilization of resources. Let’s illustrate this with an example:

**Example:**  
Number of jobs = 3; Number of processes = 2  
Execution time of process 1 = 5 secs; Execution time of process 2 = 3 secs; Execution time of process 3 = 5 secs.  
**Result:**  
Process 1 & Process 2 start concurrently with Job1 and Job2  
Job2 fails  
Job3 gets started after failure  
Job1 and Job3 end their executions despite Job2 failing

![](https://miro.medium.com/v2/resize:fit:700/0*C8hbuPTTbgjdd2Eb)

# Retry Mechanism for Failed Jobs

A retry mechanism for failed jobs is available by saving them in a failed queue and retrying them for a specified number of times. For instance, if a job fails during execution, it gets added to the failed queue. Subsequently, the failed job is retried for a set number of attempts, ensuring robustness in handling failures. Let’s explore this with an example:

**Example:**  
Number of jobs = 3; Number of processes = 2  
Execution time of process 1 = 5 secs; Execution time of process 2 = 3 secs; Execution time of process 3 = 5 secs.  
**Result:**  
Process 1 & Process 2 start concurrently with Job1 and Job2  
Job2 fails  
Job3 gets started after failure  
Job1 and Job3 end their executions despite Job2 failing  
Job2 retries for X amount of times

![](https://miro.medium.com/v2/resize:fit:700/0*jk1TaWgrX3t_70Xs)

# Custom Parallel Processing Framework

To address these challenges effectively, I have developed a custom parallel processing framework that incorporates retry functionality. This framework optimizes job execution, handles exceptions seamlessly, and offers a retry mechanism for failed jobs. With this framework, you can execute tasks concurrently, manage job queues efficiently, and ensure resilience in handling failures. Check out the attached code to explore the implementation details

```python
from concurrent.futures import ThreadPoolExecutor  
import time  
  
# Defining a worker task  
def task(job,sleep_time):  
    print(f"====>Started Job : {job}...")  
    if job == 2:  
        time.sleep(sleep_time)  
        print(f"====>Exception Occurred - Job : {job}")  
        raise Exception("Failed")  
    time.sleep(sleep_time)  
    print(f"====>Ended Job : {job}....")  
    return job  
  
  
# Define a framework for running jobs in parallel  
def run_parallel(pool,fn,all_jobs,task_times):  
    job_states = [pool.submit(fn,job,task_times[index]) for index,job in enumerate(all_jobs)]  
    successful_jobs = []  
      
    for job_state in job_states:  
        try:  
            job = job_state.result()  
            successful_jobs.append(job)  
            print(f"====>Output Job : {job}")  
        except Exception as error:  
            pass  
  
    failed_jobs_num = [index for index,job in enumerate(all_jobs) if job not in successful_jobs]  
      
    return failed_jobs_num  
  
# Define a robust way of running jobs in parallel and having retries on errors  
def run_parallel_with_retries(pool,fn,all_jobs,task_times,retries):  
      
    failed_jobs_num = run_parallel(pool,fn,all_jobs,task_times)  
      
    while retries >=0 and failed_jobs_num is not None:  
        all_jobs = [job for index,job in enumerate(all_jobs) if index in failed_jobs_num]  
        task_times = [task_time for index,task_time in enumerate(task_times) if index in failed_jobs_num]  
        print("Retries Left - {} for Jobs : {}".format(retries,all_jobs))  
        failed_jobs_num = run_parallel(pool,fn,all_jobs,task_times)  
        retries -=1  
  
  
def main():  
    # Defining the number of workers  
    num_workers = 2  
    # Defining the hypothetical task times  
    task_times = [5,2,5]  
    # Defining the jobs to be run in parallel  
    all_jobs = [1,2,3]  
    # Defining the number of retries  
    retries = 2  
      
    # Defining the multiprocessing pool  
    pool = ThreadPoolExecutor(max_workers=num_workers)  
      
    # Executing the multiprocessing framework  
    run_parallel_with_retries(pool,task,all_jobs,task_times,retries)  
  
  
if __name__ == "__main__":  
    main()
```

The architectural diagram below for a particular use case will make things more clear for how the framework works

![](https://miro.medium.com/v2/resize:fit:700/1*lZYc5YbbSKwRwCAwkjqlug.png)

In conclusion, multiprocessing in Python offers a flexible and efficient way to handle concurrent tasks. By understanding the behavior of queues, exceptions, and retries, you can build robust parallel processing solutions that enhance productivity and reliability. Stay tuned for more insights and updates on Python multiprocessing!
