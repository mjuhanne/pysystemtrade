from multiprocessing import Pool
from syscore.genutils import progressBar
from syscore.objects import arg_not_supplied
from systems.system_cache import MISSING_FROM_CACHE

# Hacky way to refer to stage without pickling it
stage_reference = None

def invoke_stage_function_in_new_process(process_number, show_progressbar, func_name, arg_array):
    ##### Execution of new process starts here ######
    
    stage = globals()['stage_reference']
    
    if show_progressbar:
        progress = progressBar(len(arg_array), "Processing %s/%d" % (func_name, process_number))
        
    # For this new process we have to reinitialize all classes using mongo
    stage.parent.data.data.reconnect_mongo_classes()

    results = dict()
    func = getattr(stage, func_name)
    
    for args in arg_array: 
        result = func(*args)
        cache_ref = stage.parent.cache.cache_ref(func, 
            stage, 
            *args, use_arg_names=True, instrument_classify=True
        )
        results[cache_ref] = result
        if show_progressbar:
            progress.iterate()

    if show_progressbar:
        progress.finished()
    return (process_number, results)


def divide_jobs_for_processes(n_processes, args, jobs, include_process_number=False):
    job_count_per_process = int(len(jobs) / n_processes)
    if job_count_per_process * n_processes < len(jobs):
        job_count_per_process += 1

    if include_process_number:
        return [ [int(i/job_count_per_process), *args, jobs[i:i + job_count_per_process]]
            for i in range(0, len(jobs), job_count_per_process) ]
    else:
        return [ [*args, jobs[i:i + job_count_per_process]]
            for i in range(0, len(jobs), job_count_per_process) ]


def parallelize_stage_function_and_cache_results(stage, func, arg_array):
    n_processes = stage.config.get_element_or_arg_not_supplied('n_processes')
    globals()['stage_reference'] = stage
    if n_processes is not arg_not_supplied:
        
        # Select only those jobs that haven't been cached already
        jobs = []
        for args in arg_array:
            cache_ref = stage.parent.cache.cache_ref(func, 
                stage, 
                *args, use_arg_names=True, instrument_classify=True
            )
            if stage_reference.parent.cache._get_item_from_cache(cache_ref) is MISSING_FROM_CACHE:
                jobs.append(args)
        
        jobs_per_process = divide_jobs_for_processes(n_processes, [True, func.__func__.__name__], jobs, include_process_number=True)
        
        with Pool(n_processes) as p:
            for i, (process_number, result_dict) in enumerate(p.starmap(invoke_stage_function_in_new_process, jobs_per_process), 1):
                print("Process %d finished with %d jobs" % (process_number, len(result_dict)))                    
                for cache_ref, result in result_dict.items():
                    # We can cache results for only this top level function. Results for lower level functions are
                    # obviously destroyed when sub-processes exit
                    stage.parent.cache.set_item_in_cache(
                        result, cache_ref, protected=False, not_pickable=False
                    )
    else:
        stage.log.warn("Process count (n_processes) not specified in private config! Skipping parallelized caching..")
    