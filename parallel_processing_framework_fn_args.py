# Parallel Execution Framework with Arbitary Number of Arguments

from concurrent.futures import ThreadPoolExecutor
import time

class ParallelExec:
    """
    Attributes:
        num_workers (int): number of workers
    """
    
    def __init__(self,num_workers):
        self._pool = ThreadPoolExecutor(max_workers=num_workers)
    
    # Getting constant and parallel arguments from all the arguments
    def get_cnst_parallel_args(self,*args):
        cnst_args = [arg for arg in args if not isinstance(arg,list)]
        parallel_args = [arg for arg in args if isinstance(arg,list)]
        return cnst_args,parallel_args
    
    # Getting arguments for a particular split
    def get_args_split(self,split_num,args):
        num_args = len(args)
        return [args[index][split_num] for index in range(0,num_args)]
    
    # Define a framework for running jobs in parallel
    def run_parallel(self,fn,*args):
        # Getting constant and parallel arguments from all arguments
        cnst_args, parallel_args = self.get_cnst_parallel_args(*args)
        
        # Getting the number of jobs which can run in parallel
        num_splits = len(parallel_args[0])
        all_jobs_indices = [index for index in range(0,num_splits)]
        
        # Getting all arguments according to parallel splits
        all_args = [tuple(cnst_args+self.get_args_split(index,parallel_args)) for index in range(0,num_splits)]
        print("All Splits : {}".format(all_args))
        
        # Submitting jobs to the pool in parallel
        job_states = [self._pool.submit(fn,*all_args[index]) for index in range(0,num_splits)]
        
        successful_jobs = []
        failed_args = []
        
        # Determining the jobs successful and failed jobs
        for index,job_state in enumerate(job_states):
            all_args = tuple(cnst_args+self.get_args_split(index,parallel_args))
            try:
                job_state.result()
                successful_jobs.append(index)
                print(f"====>Job Passed with Split :{all_args}")
            except Exception as error:
                print("E {}".format(error))
                print(f"====>Job Failed with Split :{all_args}")
                pass
        
        # Getting the list of failed jobs for retrying
        failed_jobs_indices = [job_index for job_index in all_jobs_indices if job_index not in successful_jobs]
        
        # Getting the list of failed job arguments for retrying
        for arg in parallel_args:
            failed_args.append([arg[index]for index in failed_jobs_indices])
        
        # Logging the list of failed jobs and failed job arguments for retrying
        print(f" Failed Job Numbers : {failed_jobs_indices}, Failed Jobs Parallel Splits : {failed_args}")
        
        return failed_jobs_indices,failed_args
    

    # Defining a robust way of running jobs in parallel and having retries on errors
    def run_parallel_with_retries(self,fn,*args,retries):
        # Getting the list of failed jobs and failed job arguments for retrying from the first run
        failed_jobs_indices,failed_parallel_args = self.run_parallel(fn,*args)
        # Getting constant and parallel arguments from all the arguments
        cnst_args, parallel_args = self.get_cnst_parallel_args(*args)
        
        # Retrying the failed parallel jobs in case the failed jobs exists and number of retries attempted < allowed number of retries
        while retries >=0 and failed_jobs_indices:
            # Getting all arguments of failed jobs according to parallel splits
            all_args = tuple(cnst_args+failed_parallel_args)
            print(f"All Splits being retried : {all_args}")
            
            # Running the failed jobs in parallel
            failed_jobs_indices,failed_parallel_args = self.run_parallel(fn,*all_args)
            print("Retries Left - {}".format(retries))
            
            # Reducing the number of attempts left
            retries -=1
        

# Defining the operational parameters
max_workers = 3
retries = 3

# Defining a mock function for testing functionality
def foo(c1,c2,p1,p2):
    if (c1 == 'c1' and c2 == 'c2' and p1 == 3 and p2 == 9) or (c1 == 'c1' and c2 == 'c2' and p1 == 4 and p2 == 10) :
        raise Exception("FAILED....")
    else:
        print(f"C1 : {c1} , C2 : {c2} , P1: {p1} , P2 : {p2}")

# Using the parallel exection framework
parallel_exec = ParallelExec(num_workers=max_workers)
parallel_exec.run_parallel_with_retries(foo,"c1","c2",[1,2,3,4,5,6],[7,8,9,10,11,12],retries=3)
    
