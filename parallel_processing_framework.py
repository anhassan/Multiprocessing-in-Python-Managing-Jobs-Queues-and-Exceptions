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
  
